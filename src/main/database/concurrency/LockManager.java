import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * The Lock Manager handles lock and unlock requests from transactions. The
 * Lock Manager will maintain a hashtable that is keyed on the name of the
 * table being locked. The Lock Manager will also keep a FIFO queue of requests
 * for locks that cannot be immediately granted.
 */
public class LockManager {
    private DeadlockAvoidanceType deadlockAvoidanceType;
    private HashMap<String, TableLock> tableToTableLock;

    public enum DeadlockAvoidanceType {
        None,
        WaitDie,
        WoundWait
    }

    public enum LockType {
        Shared,
        Exclusive
    }

    public LockManager(DeadlockAvoidanceType type) {
        this.deadlockAvoidanceType = type;
        this.tableToTableLock = new HashMap<String, TableLock>();
    }

    /**
     * The acquire method will grant the lock if it is compatible. If the lock
     * is not compatible, then the request will be placed on the requesters
     * queue. Once you have implemented deadlock avoidance algorithms, you
     * should instead check the deadlock avoidance type and call the
     * appropriate function that you will complete in part 2.
     * @param transaction that is requesting the lock
     * @param tableName of requested table
     * @param lockType of requested lock
     */
    public void acquire(Transaction transaction, String tableName, LockType lockType)
            throws IllegalArgumentException {
        //TODO: HW5
        if (!tableToTableLock.containsKey(tableName)) {
            tableToTableLock.put(tableName, new TableLock(lockType));
        }

        TableLock tblock = tableToTableLock.get(tableName);


        //If a blocked transaction calls acquire, throw an IllegalArgumentException.
        if (transaction.getStatus().equals(Transaction.Status.Waiting)) {
            throw new IllegalArgumentException();
        }
        //If a transaction that currently holds an exclusive lock on a table requests a shared lock on the same table, throw an IllegalArgumentException.
        if (tblock.lockType.equals(LockType.Exclusive) && tblock.lockOwners.contains(transaction)) {
            throw new IllegalArgumentException();
        }
        //If a transaction requests a lock that it already holds, throw an IllegalArgumentException.
        if (tblock.lockOwners.contains(transaction) && lockType.equals(tblock.lockType)) {
            throw new IllegalArgumentException();
        }

        boolean isCompatible = compatible(tableName, transaction, lockType);

        // Grant the lock
        if (isCompatible) {
            // Set right lock type
            tblock.lockType = lockType;
            // Add owner
            tblock.lockOwners.add(transaction);
        } else {
            if (this.deadlockAvoidanceType.equals(DeadlockAvoidanceType.WaitDie)) {
                waitDie(tableName, transaction, lockType);

            } else if (this.deadlockAvoidanceType.equals(DeadlockAvoidanceType.WoundWait)) {
                woundWait(tableName, transaction, lockType);
            } else {
                transaction.sleep();
                // Create request
                Request newReq = new Request(transaction, lockType);
                // If it is a promotion, move to the front of queue
                if (tblock.lockOwners.contains(transaction)) {
                    tblock.requestersQueue.addFirst(newReq);
                } else {
                    tblock.requestersQueue.addLast(newReq);
                }
            }

        }

    }

    /**
     * This method will return true if the requested lock is compatible. See
     * spec provides compatibility conditions.
     * @param tableName of requested table
     * @param transaction requesting the lock
     * @param lockType of the requested lock
     * @return true if the lock being requested does not cause a conflict
     */
    private boolean compatible(String tableName, Transaction transaction, LockType lockType) {
        //TODO: HW5 Implement
        TableLock tblock = tableToTableLock.get(tableName);
        //If no other transaction currently holds a lock on the table, then T's lock request is granted.
        if (tblock.lockOwners.isEmpty()) {
            return true;
        }

        Transaction lockOwner = tblock.lockOwners.iterator().next();
        //If T holds a shared lock on the table, and no other transaction holds a shared lock on the table,
        //and T is requesting an exclusive lock, then T's lock request is granted. Its lock is promoted from a shared lock to an exclusive lock.
        if (lockOwner.equals(transaction) && tblock.lockOwners.size() == 1 && tblock.lockType.equals(LockType.Shared)) {
            return true;
        }

        //If some other transaction currently holds an exclusive lock on the table, then T's lock request is denied and T blocks.
        if (tblock.lockType == LockType.Exclusive) {
            return false;
        }
        //If some other transactions currently hold a shared lock on the table, and T is requesting a shared lock, then T's lock request is granted.
        if (!lockOwner.equals(transaction) && lockType == LockType.Shared) {
            return true;
        }
        //If some other transactions currently hold a shared lock on the table,
        //and T is requesting an exclusive lock, then T's lock request is denied and T blocks.
        if (!lockOwner.equals(transaction) && lockType == LockType.Exclusive) {
            return false;
        }

        // Tries to get the same lock that it already has will fall into this case
        return false;
    }

    /**
     * Will release the lock and grant all mutually compatible transactions at
     * the head of the FIFO queue. See spec for more details.
     * @param transaction releasing lock
     * @param tableName of table being released
     */
    public void release(Transaction transaction, String tableName) throws IllegalArgumentException{
        //TODO: HW5 Implement
        TableLock tblock = tableToTableLock.get(tableName);
        //If a transaction tries to release a lock it does not hold, throw an IllegalArgumentException.
        if (tblock == null) {
            throw new IllegalArgumentException();
        }
        //If a blocked transaction calls release, throw an IllegalArgumentException.
        if (!tblock.lockOwners.contains(transaction)) {
            throw new IllegalArgumentException();
        }


        tblock.lockOwners.remove(transaction);

        //If there is a single shared owner of the lock, and that owner has an exclusive lock request
        // in requestersQueue, we grant the lock request. This is a lock promotion.
        if (tblock.lockOwners.size() == 1) {
            Transaction lockOwner = tblock.lockOwners.iterator().next();
            for (Request r : tblock.requestersQueue) {
                if (r.transaction.equals(lockOwner) && r.lockType.equals(LockType.Exclusive)) {
                    r.transaction.wake();
                    acquire(r.transaction, tableName, r.lockType);
                    tblock.requestersQueue.remove(r);
                    return;
                }
            }
        }



        //If there are multiple shared owners of the lock, then all shared requests in requestersQueue should be granted.
        if (tblock.lockOwners.size() > 1) {
            for (Request r : tblock.requestersQueue) {
                if (r.lockType.equals(LockType.Shared)) {
                    tblock.requestersQueue.remove(r);
                    r.transaction.wake();
                    acquire(r.transaction, tableName, r.lockType);
                }
            }
        }

        //If no transaction owns the lock
        if (tblock.lockOwners.isEmpty() && !tblock.requestersQueue.isEmpty()) {
            Request firstReq = tblock.requestersQueue.getFirst();
            // if the first request in requestersQueue is a shared request, then all shared requests should be granted.
            if (firstReq.lockType.equals(LockType.Shared)) {
                tblock.lockType = LockType.Shared;
                int i = 0;
                while (i < tblock.requestersQueue.size()) {
                    Request otherReq = tblock.requestersQueue.get(i);
                    if (otherReq.lockType.equals(LockType.Shared)) {
                        tblock.requestersQueue.remove(i);
                        otherReq.transaction.wake();
                        acquire(otherReq.transaction, tableName, otherReq.lockType);
                    } else {
                        i++;
                    }
                }
                // if the first request in requestersQueue is an exclusive request, that exclusive request should be granted.
            } else {
                tblock.requestersQueue.remove(0);
                firstReq.transaction.wake();
                acquire(firstReq.transaction, tableName, firstReq.lockType);
                tblock.lockType = LockType.Exclusive;
            }

        }

        //If the lock has no owners and requestersQueue is empty, remove the lock from the tableToTableLock map.
        if (tblock.lockOwners.isEmpty() && tblock.requestersQueue.isEmpty()) {
            tableToTableLock.remove(tableName);
        }

    }

    /**
     * Will return true if the specified transaction holds a lock of type
     * lockType on the table tableName.
     * @param transaction holding lock
     * @param tableName of locked table
     * @param lockType of lock
     * @return true if the transaction holds lock
     */
    public boolean holds(Transaction transaction, String tableName, LockType lockType) {
        //TODO: HW5 Implement
        TableLock tblock = tableToTableLock.get(tableName);
        if (tblock == null) {
            return false;
        }
        if (tblock.lockOwners.contains(transaction) && lockType.equals(tblock.lockType)) {
            return true;
        }
        return false;
    }

    /**
     * If transaction t1 requests an incompatible lock, t1 will abort if it has
     * a lower priority (higher timestamp) than all conflicting transactions.
     * If t1 has a higher priority, it will wait on the requesters queue.
     * @param tableName of locked table
     * @param transaction requesting lock
     * @param lockType of request
     */
    private void waitDie(String tableName, Transaction transaction, LockType lockType) {
        //TODO: HW5 Implement
        TableLock tblock = tableToTableLock.get(tableName);
        boolean lowestPriority = true;

        Iterator<Transaction> iter = tblock.lockOwners.iterator();
        while (iter.hasNext()) {
            Transaction owner = iter.next();
            if (owner.getTimestamp() >= transaction.getTimestamp()) {
                lowestPriority = false;
                break;
            }
        }

        if (lowestPriority) {
            transaction.abort();
        } else {
            transaction.sleep();
            // Create request
            Request newReq = new Request(transaction, lockType);
            // If it is a promotion, move to the front of queue
            if (tblock.lockOwners.contains(transaction)) {
                tblock.requestersQueue.addFirst(newReq);
            } else {
                tblock.requestersQueue.addLast(newReq);
            }
        }


    }

    /**
     * If transaction t1 requests an incompatible lock, t1 will wait if it has
     * a lower priority (higher timestamp) than conflicting transactions. If t1
     * has a higher priority than every conflicting transaction, it will abort
     * all the lock holders and acquire the lock.
     * @param tableName of locked table
     * @param transaction requesting lock
     * @param lockType of request
     */
    private void woundWait(String tableName, Transaction transaction, LockType lockType) {
        //TODO: HW5 Implement
        TableLock tblock = tableToTableLock.get(tableName);
        boolean highestPriority = true;

        Iterator<Transaction> iter = tblock.lockOwners.iterator();
        while (iter.hasNext()) {
            Transaction owner = iter.next();
            if (owner.getTimestamp() <= transaction.getTimestamp()) {
                highestPriority = false;
                break;
            }
        }

        if (highestPriority) {
            Iterator<Transaction> iter2 = tblock.lockOwners.iterator();
            while (iter2.hasNext()) {
                Transaction aborted = iter2.next();
                aborted.abort();
                removeRequestersAborted(aborted, tblock);
            }
            tblock.lockOwners.clear();
            tblock.lockOwners.add(transaction);
            tblock.lockType = lockType;
        } else {
            transaction.sleep();
            // Create request
            Request newReq = new Request(transaction, lockType);
            // If it is a promotion, move to the front of queue
            if (tblock.lockOwners.contains(transaction)) {
                tblock.requestersQueue.addFirst(newReq);
            } else {
                tblock.requestersQueue.addLast(newReq);
            }
        }

    }

    private void removeRequestersAborted(Transaction trs, TableLock tblock) {
        for (Request rqst : tblock.requestersQueue) {
            if (rqst.transaction.equals(trs)) {
                tblock.requestersQueue.remove(rqst);
            }
        }
    }

    /**
     * Contains all information about the lock for a specific table. This
     * information includes lock type, lock owner(s), and lock requestor(s).
     */
    private class TableLock {
        private LockType lockType;
        private HashSet<Transaction> lockOwners;
        private LinkedList<Request> requestersQueue;

        public TableLock(LockType lockType) {
            this.lockType = lockType;
            this.lockOwners = new HashSet<Transaction>();
            this.requestersQueue = new LinkedList<Request>();
        }

    }

    /**
     * Used to create request objects containing the transaction and lock type.
     * These objects will be added to the requestor queue for a specific table
     * lock.
     */
    private class Request {
        private Transaction transaction;
        private LockType lockType;

        public Request(Transaction transaction, LockType lockType) {
            this.transaction = transaction;
            this.lockType = lockType;
        }
    }
}
