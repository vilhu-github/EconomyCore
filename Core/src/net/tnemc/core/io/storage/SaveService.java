package net.tnemc.core.io.storage;

/*
 * The New Economy
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 */

import net.tnemc.core.TNECore;
import net.tnemc.core.account.Account;
import net.tnemc.core.transaction.Receipt;
import net.tnemc.core.transaction.Transaction;
import net.tnemc.plugincore.PluginCore;
import net.tnemc.plugincore.core.compatibility.log.DebugLevel;
import net.tnemc.plugincore.core.io.storage.Datable;
import net.tnemc.plugincore.core.io.storage.StorageConnector;
import net.tnemc.plugincore.core.io.storage.StorageManager;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Runs every TNE write on one dedicated thread.
 *
 * <p>A single writer means saves never overlap, never race each other for the same account, and
 * never pile up as new Bukkit scheduler threads. Reloads and shutdown {@link #flush() flush} through
 * the same thread so no queued or unsaved change is overwritten or dropped.</p>
 */
public final class SaveService {

  private static final long FLUSH_TIMEOUT_SECONDS = 120;
  //Below Paper's 60 second watchdog: reloads wait on the server thread.
  private static final long RELOAD_TIMEOUT_SECONDS = 20;

  private volatile Thread storageThread;

  private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(runnable->{
    final Thread thread = new Thread(runnable, "TNE-Storage");
    thread.setDaemon(true);
    storageThread = thread;
    return thread;
  });

  /**
   * Starts saving modified data periodically. Each run starts only after the previous one ended.
   *
   * @param intervalSeconds Seconds between the end of one save and the start of the next.
   */
  public void start(final long intervalSeconds) {

    if(intervalSeconds > 0) {
      executor.scheduleWithFixedDelay(guard(()->{
        PluginCore.log().inform("Running AutoSaver.");
        saveDirty();
      }), intervalSeconds, intervalSeconds, TimeUnit.SECONDS);
    }
  }

  /**
   * Queues a save of one account, regardless of whether it is marked as modified.
   */
  public void save(final Account account) {

    run(()->datable(Account.class).ifPresent(datable->datable.store(connector(), account,
                                                                        account.getIdentifier().toString())));
  }

  /**
   * Queues a save of every modified account and receipt.
   */
  public void saveAll() {

    run(this::saveDirty);
  }

  /**
   * Queues storage work on the storage thread.
   */
  public void run(final Runnable task) {

    try {
      executor.execute(guard(task));
    } catch(final RejectedExecutionException shutDown) {
      guard(task).run();
    }
  }

  /**
   * Saves all unsaved changes, then replaces in-memory data with database data. No balance change
   * can happen in between, and nothing is replaced unless every account was saved.
   *
   * @param reload Replaces in-memory data, e.g. by reloading accounts from the database.
   *
   * @return false if saving failed and the reload was cancelled.
   */
  public boolean reload(final Runnable reload) {

    synchronized(Transaction.BALANCE_LOCK) {
      if(!flush(RELOAD_TIMEOUT_SECONDS)) {
        PluginCore.log().error("Reload cancelled: unsaved balance changes could not be saved.", DebugLevel.OFF);
        return false;
      }
      reload.run();
      return true;
    }
  }

  /**
   * Waits until all queued work has finished and every modified account and receipt was saved.
   *
   * @return true if no account is left with unsaved changes.
   */
  private boolean flush(final long timeoutSeconds) {

    if(Thread.currentThread() == storageThread) {
      saveDirty();
      return accountsSaved();
    }

    try {
      executor.submit(this::saveDirty).get(timeoutSeconds, TimeUnit.SECONDS);
    } catch(final RejectedExecutionException shutDown) {
      guard(this::saveDirty).run();
    } catch(final TimeoutException e) {
      PluginCore.log().error("TNE storage did not finish saving within " + timeoutSeconds + " seconds.", DebugLevel.OFF);
      return false;
    } catch(final InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch(final ExecutionException e) {
      PluginCore.log().error("TNE storage flush failed.", e, DebugLevel.OFF);
      return false;
    }
    return accountsSaved();
  }

  /**
   * Failed saves keep their account dirty. Receipts are left out on purpose: they only hold
   * history, and one receipt the database rejects must not block every reload.
   */
  private static boolean accountsSaved() {

    return TNECore.eco().account().getAccounts().values().stream().noneMatch(Account::isDirty);
  }

  /**
   * Saves everything, stops the storage thread and releases plugin core's storage worker.
   */
  public void shutdown() {

    if(!flush(FLUSH_TIMEOUT_SECONDS)) {
      PluginCore.log().error("TNE could not save every account before shutdown; unsaved balance changes are lost.",
                             DebugLevel.OFF);
    }
    executor.shutdown();
    try {
      executor.awaitTermination(FLUSH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch(final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    stopPluginCoreWorker();
  }

  private void saveDirty() {

    markOnlinePlayersDirty();
    datable(Account.class).ifPresent(datable->datable.storeAll(connector(), null));
    datable(Receipt.class).ifPresent(datable->datable.storeAll(connector(), null));
  }

  /**
   * Item balances live in player inventories, so the wallet is not marked as modified when items
   * are picked up or dropped. Online players are always saved so the stored value stays current.
   */
  private void markOnlinePlayersDirty() {

    if(TNECore.eco().currency().currencies().stream().noneMatch(currency->currency.type().supportsItems())) {
      return;
    }

    for(final String name : onlinePlayers()) {
      TNECore.eco().account().findAccount(name).ifPresent(Account::markDirty);
    }
  }

  private Collection<String> onlinePlayers() {

    try {
      return PluginCore.server().onlinePlayersList();
    } catch(final RuntimeException e) {
      return Collections.emptyList();
    }
  }

  private Runnable guard(final Runnable task) {

    return ()->{
      try {
        task.run();
      } catch(final Throwable e) {
        //An escaped exception would cancel the periodic autosave for good.
        PluginCore.log().error("TNE storage task failed.", e instanceof final Exception ex? ex : new Exception(e), DebugLevel.OFF);
      }
    };
  }

  @SuppressWarnings("unchecked")
  private static <T> Optional<Datable<T>> datable(final Class<T> type) {

    return Optional.ofNullable((Datable<T>)TNECore.instance().storage().getEngine().datables().get(type));
  }

  private static StorageConnector<?> connector() {

    return TNECore.instance().storage().getConnector();
  }

  /**
   * Plugin core's storage worker blocks on its queue and ignores executor shutdown, which stalled
   * every server stop for 10 seconds. A final queued task stops it once earlier queued work ran.
   */
  private static void stopPluginCoreWorker() {

    final StorageManager storage = StorageManager.instance();
    if(storage == null) {
      return;
    }
    storage.getEngine().datables().put(WorkerStop.class, new WorkerStopDatable());
    storage.store(new WorkerStop(Thread.currentThread()), "");
  }

  private record WorkerStop(Thread caller) { }

  private static final class WorkerStopDatable implements Datable<WorkerStop> {

    @Override
    public Class<? extends WorkerStop> clazz() {

      return WorkerStop.class;
    }

    @Override
    public void purge(final StorageConnector<?> connector) {
    }

    @Override
    public void store(final StorageConnector<?> connector, final WorkerStop stop, final String identifier) {

      if(Thread.currentThread() != stop.caller()) {
        Thread.currentThread().interrupt();
      }
    }

    @Override
    public void storeAll(final StorageConnector<?> connector, final String identifier) {
    }

    @Override
    public void delete(final StorageConnector<?> connector, final String identifier) {
    }

    @Override
    public Optional<WorkerStop> load(final StorageConnector<?> connector, final String identifier) {

      return Optional.empty();
    }

    @Override
    public Collection<WorkerStop> loadAll(final StorageConnector<?> connector, final String identifier) {

      return Collections.emptyList();
    }
  }
}
