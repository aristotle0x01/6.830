package simpledb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 
 * lock manager provides lock service to transactions 
 * when read/write buffer pool page
 * 
 */
public class LockManager {
	// pages s_locked by a transaction
	private final Map<TransactionId,Set<PageId>> txLocks_s;
	
	// pages x_locked by a transaction
	private final Map<TransactionId,Set<PageId>> txLocks_x;
	
	// pages hold shared lock
	private final Map<PageId,Set<TransactionId>> s_pages;
	
	// pages hold exclusive lock
	private final Map<PageId,Set<TransactionId>> x_pages;
	
	
	public LockManager(){
		txLocks_s = new HashMap<>();
		txLocks_x = new HashMap<>();
		s_pages = new HashMap<>();
		x_pages = new HashMap<>();
	}
	
	public synchronized boolean holdsLock(TransactionId tid, PageId p){
		if(s_pages.containsKey(p) && s_pages.get(p).contains(tid)){
			return true;
		}
		
		if(x_pages.containsKey(p) && x_pages.get(p).contains(tid)){
			return true;
		}
		
		return false;
	}
	
	public void lock(TransactionId tid, PageId p, Permissions perm){
		while(!doLock(tid,p,perm)){
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
			}
		}
	}
	
	private synchronized boolean doLock(TransactionId tid, PageId p, Permissions perm){
		if(perm.equals(Permissions.READ_ONLY)){
			if(x_pages.containsKey(p)){
				// txs不会为空
				Set<TransactionId> txs = x_pages.get(p);
				if(txs.contains(tid)){
					//已有write lock
					return true;
				}else{
					// block, wait to get read lock
					return false;
				}
			}else{
				if(s_pages.containsKey(p)){
					s_pages.get(p).add(tid);
				}else{
					Set<TransactionId> nset = new HashSet<>();
					nset.add(tid);
					s_pages.put(p, nset);
				}
				
				if(txLocks_s.containsKey(tid)){
					txLocks_s.get(tid).add(p);
				}else{
					Set<PageId> pSet = new HashSet<>();
					pSet.add(p);
					
					txLocks_s.put(tid, pSet);
				}
				
				return true;
			}
		}else{
			if(x_pages.containsKey(p)){
				// txs不会为空
				Set<TransactionId> txs = x_pages.get(p);
				if(txs.contains(tid)){
					//已有write lock
					return true;
				}else{
					// block, wait to get write lock
					return false;
				}
			}else{
				if(s_pages.containsKey(p)){
					Set<TransactionId> tSet = s_pages.get(p);
					if(tSet.contains(tid)){
						if(tSet.size() == 1){
							// grant write lock
							Set<TransactionId> nset = new HashSet<>();
							nset.add(tid);
							x_pages.put(p, nset);
							
							if(txLocks_x.containsKey(tid)){
								txLocks_x.get(tid).add(p);
							}else{
								Set<PageId> pSet = new HashSet<>();
								pSet.add(p);
								txLocks_x.put(tid, pSet);
							}
							
							// release read lock
							s_pages.get(p).remove(tid);
							if(s_pages.get(p).isEmpty()){
								s_pages.remove(p);
							}
							
							txLocks_s.get(tid).remove(p);
							if(txLocks_s.get(tid).isEmpty()){
								txLocks_s.remove(tid);
							}
							
							return true;
						}else{
							// block
							return false;
						}
					}else{
						// block
						return false;
					}
				}else{
					Set<TransactionId> nset = new HashSet<>();
					nset.add(tid);
					x_pages.put(p, nset);
					
					if(txLocks_x.containsKey(tid)){
						txLocks_x.get(tid).add(p);
					}else{
						Set<PageId> pSet = new HashSet<>();
						pSet.add(p);
						txLocks_x.put(tid, pSet);
					}
					
					return true;
				}
			}
		}
	}
	
	// unlock page p by transaction tid
	public synchronized void unlock(TransactionId tid, PageId p){
		if(txLocks_s.containsKey(tid)){
			txLocks_s.get(tid).remove(p);
			if(txLocks_s.get(tid).isEmpty()){
				txLocks_s.remove(tid);
			}
		}
		
		if(txLocks_x.containsKey(tid)){
			txLocks_x.get(tid).remove(p);
			if(txLocks_x.get(tid).isEmpty()){
				txLocks_x.remove(tid);
			}
		}
		
		if(s_pages.containsKey(p)){
			s_pages.get(p).remove(tid);
			if(s_pages.get(p).isEmpty()){
				s_pages.remove(p);
			}
		}
		
		if(x_pages.containsKey(p)){
			x_pages.get(p).remove(tid);
			if(x_pages.get(p).isEmpty()){
				x_pages.remove(p);
			}
		}
	}

	public synchronized void unlock(TransactionId tid){
		if(txLocks_s.containsKey(tid)){
			Set<PageId> sSet = txLocks_s.get(tid);
			for(PageId id: sSet){
				if(s_pages.containsKey(id)){
					s_pages.get(id).remove(tid);
					if(s_pages.get(id).isEmpty()){
						s_pages.remove(id);
					}
				}
			}
			
			txLocks_s.remove(tid);
		}
		
		if(txLocks_x.containsKey(tid)){
			Set<PageId> tSet = txLocks_x.get(tid);
			for(PageId id: tSet){
				if(x_pages.containsKey(id)){
					x_pages.get(id).remove(tid);
					if(x_pages.get(id).isEmpty()){
						x_pages.remove(id);
					}
				}
			}
			
			txLocks_x.remove(tid);
		}
	}
	
	public synchronized void unlock(PageId pid){
		if(s_pages.containsKey(pid)){
			Set<TransactionId> tids = s_pages.get(pid);
			for(TransactionId tid: tids){
				if(txLocks_s.containsKey(tid)){
					txLocks_s.get(tid).remove(pid);
					if(txLocks_s.get(tid).isEmpty()){
						txLocks_s.remove(tid);
					}
				}
			}
			
			s_pages.remove(pid);
		}
		
		if(x_pages.containsKey(pid)){
			Set<TransactionId> tids = x_pages.get(pid);
			for(TransactionId tid: tids){
				if(txLocks_x.containsKey(tid)){
					txLocks_x.get(tid).remove(pid);
					if(txLocks_x.get(tid).isEmpty()){
						txLocks_x.remove(tid);
					}
				}
			}
			
			x_pages.remove(pid);
		}
	}
}



