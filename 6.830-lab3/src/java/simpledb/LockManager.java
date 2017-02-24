package simpledb;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 
 * lock manager provides lock service to transactions 
 * when read/write buffer pool page
 * 
 */
public class LockManager {
	
	// pageid.hashcode -> Item
	private final HashMap<Integer,Item> manager;
	
	public LockManager(){
		manager = new HashMap<>();
	}
	
	public synchronized boolean holdsLock(TransactionId tid, PageId p){
		if(!manager.containsKey(p.hashCode())){
			return false;
		}
		
		Item item = manager.get(p.hashCode());
		return item.trans.containsKey(tid.hashCode());
	}
	
	private void doLock(Item locks, Permissions perm){
		if(perm.equals(Permissions.READ_ONLY)){
			locks.lock.readLock().lock();
		}else{
			locks.lock.writeLock().lock();
		}
	}
	
	public synchronized void lock(TransactionId tid, PageId p, Permissions perm){
		Integer phash = p.hashCode();
		Integer thash = tid.hashCode();
		
		System.out.println("1 " + thash +" "+phash + " " + perm.permLevel);
		
		// 页面尚未被锁定
		if(!manager.containsKey(phash)){
			Item locks  = new Item();
			doLock(locks, perm);
			locks.trans.put(thash, perm);
			manager.put(phash,locks);
			return;
		}
		
		Item locks = manager.get(phash);
		if(locks.trans.isEmpty()){
			doLock(locks, perm);
			locks.trans.put(thash, perm);
		}else{
			if(locks.trans.containsKey(thash)){
				Permissions temp = locks.trans.get(thash);
				if(temp.equals(Permissions.READ_ONLY)){
					if(locks.trans.size() ==  1){
						if(perm.equals(Permissions.READ_WRITE)){
							// 释放既有读锁，再加写锁
							System.out.println("2 " + thash +" "+phash + " " + perm.permLevel);
							locks.lock.readLock().unlock();
							doLock(locks, perm);
						}
						locks.trans.put(thash, perm);
					}else{
						if(perm.equals(Permissions.READ_ONLY)){
							// 已有读锁，do nothing
						}else{
							System.out.println("3 " + thash +" "+phash + " " + perm.permLevel);
							// 释放既有读锁，再加写锁
							locks.lock.readLock().unlock();
							doLock(locks, perm);
							locks.trans.put(thash, perm);
						}
					}
				}else{
					// can READ_WRITE already
					return;
				}
			}else{
				if(perm.equals(Permissions.READ_WRITE)){
					// block
					doLock(locks, perm);
					locks.trans.put(thash, perm);
				}else{
					if(locks.trans.size() > 1){
						doLock(locks, perm);
						locks.trans.put(thash, perm);
					}else{
						// actually just one element
						Collection<Permissions> clist = locks.trans.values();
						for(Permissions c: clist){
							if(c.equals(Permissions.READ_WRITE)){
								// block
								doLock(locks, perm);
								locks.trans.put(thash, perm);
							}else{
								doLock(locks, perm);
								locks.trans.put(thash, perm);
							}
						}
					}
				}
			}
		}
	}
	
	public synchronized void unlock(TransactionId tid, PageId p){
		Integer pageid = p.hashCode();
		if(!manager.containsKey(pageid)){
			return;
		}
		
		Item locks = manager.get(pageid);
		if(locks.trans.containsKey(tid.hashCode())){
			Permissions perm = locks.trans.get(tid.hashCode());
			if(perm.equals(Permissions.READ_ONLY)){
				locks.lock.readLock().unlock();
			}else{
				locks.lock.writeLock().unlock();
			}
			locks.trans.remove(tid.hashCode());
			
			if(locks.trans.isEmpty()){
				// current page no longer holds a lock, just delete it
				manager.remove(pageid);
			}
		}
	}

	public synchronized void unlock(TransactionId tid){
		Set<Integer> pages = manager.keySet();
		Integer thash = tid.hashCode();
		
		for(Integer pid:pages){
			Item locks = manager.get(pid);
			if(locks.trans.containsKey(thash)){
				Permissions perm = locks.trans.get(thash);
				if(perm.equals(Permissions.READ_ONLY)){
					locks.lock.readLock().unlock();
				}else{
					locks.lock.writeLock().unlock();
				}
				locks.trans.remove(thash);
				
				if(locks.trans.isEmpty()){
					// current page no longer holds a lock, just delete it
					manager.remove(pid);
				}
			}
		}
	}
	
	// encapsulate all transactions that lock a certain page
	private static class Item{
		// read/write lock for a certain page
		private final ReadWriteLock lock = new ReentrantReadWriteLock();
		
		// transaction id hashcode -> permission
		private final Map<Integer,Permissions> trans = new HashMap<>();
	}
}



