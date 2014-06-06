package bufmgr;

import diskmgr.*;

import java.awt.Frame;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;

import javax.xml.crypto.dsig.keyinfo.PGPData;

import chainexception.ChainException;

import global.*;
import diskmgr.*;

public class BufMgr {
	int testt = 0;
	// String resembles the policy candidate type.
	private String policy;

	// private static integer resembles the size of the busy frames.
	private static int bufferBusySize;

	// 2D byte array resembles the buffer manager array of frames
	private static byte buffArr[][] = null;

	// resembles the type of the substitution policy.
	private static String type;

	// Buffer Descriptor array to map from frame to page.
	private static Descriptor buffDescr[];

	// Hash map to map from page ---> frame/.
	private static HashMap<Integer, Integer> pageToFrame;

	// Queue resembles clock and LRU replace Policy.
	private static Queue<Integer> lru;
	// Stack resembles MRU.
	private static Stack<Integer> mru;

	// map used with love/hate policy :
	// to keep track with the loved & hated pages.
	private static HashMap<Integer, Boolean> love_Hate;

	// constructor

	public BufMgr(int numBufs, String replaceArg) {
		buffArr = new byte[numBufs][Page.MINIBASE_PAGESIZE];
		type = replaceArg;
		policy = type;
		bufferBusySize = 0;
		buffDescr = new Descriptor[numBufs];
		pageToFrame = new HashMap<Integer, Integer>();
		lru = new LinkedList<Integer>();
		mru = new Stack<Integer>();
		love_Hate = new HashMap<Integer, Boolean>();

		for (int i = 0; i < buffArr.length; i++) {
			buffArr[i] = new byte[Page.MINIBASE_PAGESIZE];
			buffDescr[i] = new Descriptor();
		}
	}

	/**
	 * Unpin a page specified by a pageId. This method should be called with
	 * dirty == true if the client has modified the page. If so, this call
	 * should set the dirty bit for this frame. Further, if pin_count > 0, this
	 * method should decrement it. If pin_count = 0 before this call, throw an
	 * excpetion to report error. (for testing purposes, we ask you to throw an
	 * exception named PageUnpinnedExcpetion in case of error.)
	 * 
	 * @param pgid
	 *            page number in the minibase
	 * @param dirty
	 *            the dirty bit of the frame.
	 * @throws PageUnpinnedExcpetion
	 * @throws ChainException
	 * @throws Exception
	 */

	public void unpinPage(PageId pgid, boolean dirty, boolean loved)
			throws ChainException {

		// no need to check that page is dirty , just set the dirty bit with the
		// dirty boolean sent in the method

		if (pageToFrame.containsKey(pgid.pid)) {

			int index = pageToFrame.get(pgid.pid);
			buffDescr[index].setDirty(dirty);

			// check pin count
			int oldCount = buffDescr[index].getPin_count();
			if (oldCount > 0) {
				buffDescr[index].setPin_count(oldCount - 1);
			} else {
				throw new PageUnpinnedExcpetion(null,
						"Page count is zero or less");
			}

			if (policy.equalsIgnoreCase("love/hate")) {
				if (loved) {
					lru.remove(pgid.pid);
					love_Hate.put(pgid.pid, true);
				}
				//else : I'll do nothing if hated except decreasing the pincount.
			}

		} else {
			throw new HashEntryNotFoundException(null,
					"bufmgr.HashEntryNotFoundException");
		}

	}

	/**
	 * Pin a page
	 * 
	 * First check if this page is already in the buffer pool. If it is,
	 * increment the pin_count and return pointer to this page.
	 * 
	 * If the pin_count was 0 before the call, the page was a replacement
	 * candidate, but is no longer a candidate. If the page is not in the pool,
	 * choose a frame (from the set of replacement candidates) to hold this
	 * page, read the page (using the appropriate method from diskmgr package)
	 * and pin it. Also, must write out the old page in chosen frame if it is
	 * dirty before reading new page. (You can assume that emptyPage == false
	 * for this assignment.)
	 * 
	 * @param pgid
	 *            page number in the minibase.
	 * @param page
	 *            the pointer point to the page.
	 * @param emptyPage
	 *            true (empty page), false (nonempty page).
	 * @throws OutOfSpaceException
	 * @throws BufferPoolExceededException
	 * @throws ChainException
	 */
	public void pinPage(PageId pageno, Page page, boolean emptyPage,
			boolean loved) throws OutOfSpaceException,
			BufferPoolExceededException {

		// check if the page is in the pool.
		if (pageToFrame.containsKey(pageno.pid)) {

			int index = pageToFrame.get(pageno.pid);

			// increment the counter
			int oldCount = buffDescr[index].getPin_count();
			buffDescr[index].setPin_count(oldCount + 1);
			// return a pointer to the page.
			page.setpage(buffArr[index]);

			// do policy updates
			updatePolicy(pageno.pid);

		} else {
			if (isBufferFull()) {
				// throw exception
				throw new BufferPoolExceededException(null,
						"bufmgr.BufferPoolExceededException");
			}
			// page is not in the pool.

			// 1.there's a free space in the pool
			if (bufferBusySize < buffDescr.length) {

				// update the descriptor
				buffDescr[bufferBusySize].setDirty(false);
				buffDescr[bufferBusySize].setPageNumber(pageno.pid);
				buffDescr[bufferBusySize].setPin_count(1);

				// update the map
				pageToFrame.put(pageno.pid, bufferBusySize);
				// make pointer to free place
				page.setpage(buffArr[bufferBusySize]);
				// increment the buffer filled size
				bufferBusySize++;
				// do policy updates
				addMemberPolicy(pageno.pid);
				// now read new page.
				try {
					SystemDefs.JavabaseDB.read_page(pageno, page);
				} catch (InvalidPageNumberException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (FileIOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else {
				// 2. there's no free space

				// check if there's a replacement candidate.
				if (isBufferFull()) {
					// throw exception
					throw new BufferPoolExceededException(null,
							"bufmgr.BufferPoolExceededException");
				} else {
					// buffer pool has replacement candidates
					// get first replacement index
					int replacementCandidate = getReplacement();

					// check if old page dirty --> write it to disk

					if (buffDescr[replacementCandidate].isDirty()) {
						// flush page to disk
						try {
							flushPage(buffDescr[replacementCandidate]
									.getPageID());
						} catch (ChainException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}

					// remove old from the map,lru,mru
					int id = buffDescr[replacementCandidate].getPageNumber();
					pageToFrame.remove(id);

					// replace old page frame in pool.
					buffDescr[replacementCandidate].setDirty(false);
					buffDescr[replacementCandidate].setPageNumber(pageno.pid);
					buffDescr[replacementCandidate].setPin_count(1);

					// add in the map
					pageToFrame.put(pageno.pid, replacementCandidate);

					// update policy.
					addMemberPolicy(pageno.pid);

					// set new page with the old frame & read page.
					page.setpage(buffArr[replacementCandidate]);

					try {
						SystemDefs.JavabaseDB.read_page(pageno, page);
					} catch (InvalidPageNumberException | FileIOException
							| IOException e) {

						e.printStackTrace();
					}

				}
			}
		}

	}

	// this method updates policy when new element is added
	// 1.if there's free space.
	// 2.after replacing a replacement candidate.
	private void addMemberPolicy(int pid) {

		if (policy.equalsIgnoreCase("clock") || policy.equalsIgnoreCase("lru")) {
			lru.add(pid);
		} else if (policy.equalsIgnoreCase("mru")) {
			mru.push(pid);
		} else if (policy.equalsIgnoreCase("love/hate")) {
			// add at both and add to map with default loved=false.
			lru.add(pid);
			mru.add(pid);
			love_Hate.put(pid, false);
		}
	}

	// this method updates the policy of page already in the pool.
	private void updatePolicy(int pid) {
		// nothing will be done in Clock Policy.

		if (policy.equalsIgnoreCase("lru")) {
			lru.remove(pid);
			lru.add(pid);
		} else if (policy.equalsIgnoreCase("mru")) {
			mru.removeElement(pid);
			mru.push(pid);
		} else if (policy.equalsIgnoreCase("love/hate")) {
			if (love_Hate.get(pid)) {
				// page was un-pinned as loved
				// remove from stack then put it on top
				mru.remove(pid);
				mru.add(pid);
			} else {
				// page has hated history
				// dequeue then queue again at both
				lru.remove(pid);
				lru.add(pid);
				mru.remove(pid);
				mru.add(pid);
			}
		}

	}

	private int getReplacement() {

		int replacementIndex = 0;
		if (policy.equalsIgnoreCase("clock") || policy.equalsIgnoreCase("lru")) {
			int limit = lru.size();
			Queue<Integer> temp = new LinkedList<>();

			// to stop after the first found count
			boolean first = false;
			for (int i = 0; i < limit; i++) {
				int pid = lru.peek();
				int indexInArr = pageToFrame.get(pid);
				if (!first && buffDescr[indexInArr].getPin_count() == 0) {
					first = true;
					replacementIndex = indexInArr;
				}
				// get elements to the temp queue
				temp.add(lru.poll());
			}

			// return again to the lru
			for (int i = 0; i < limit; i++) {
				lru.add(temp.poll());
			}
			if (first) {
				int id = buffDescr[replacementIndex].getPageNumber();
				// System.out.println("size"+lru.size());
				lru.remove(id);
			}
		} else if (policy.equalsIgnoreCase("mru")) {
			int limit = mru.size();
			Stack<Integer> temp = new Stack<Integer>();
			boolean first = false;
			for (int i = 0; i < limit; i++) {
				int pid = mru.peek();
				int indexArr = pageToFrame.get(pid);
				if (!first && buffDescr[indexArr].getPin_count() == 0) {
					first = true;
					replacementIndex = indexArr;
				}
				temp.add(mru.pop());
			}
			// return again to the mru
			for (int i = 0; i < limit; i++) {
				mru.add(temp.pop());
			}
			if (first) {
				int id = buffDescr[replacementIndex].getPageNumber();
				// System.out.println("mru");
				// for(int jj=0;jj<mru.size();jj++)
				// {
				// System.out.print(mru.get(jj) + " ");
				// }
				// System.out.println("id"+id);
				mru.removeElement(id);
			}
		} else if (policy.equalsIgnoreCase("love/hate")) {
			boolean found = false;
			int limit = lru.size();
			Queue<Integer> temp = new LinkedList<>();
			// to stop after the first found count
			boolean first = false;
			for (int i = 0; i < limit; i++) {
				int pid = lru.peek();
				int indexInArr = pageToFrame.get(pid);
				if (!first && buffDescr[indexInArr].getPin_count() == 0) {
					first = true;
					replacementIndex = indexInArr;
					found = true;
				}
				// get elements to the temp queue
				temp.add(lru.poll());
			}

			// return again to the lru
			for (int i = 0; i < limit; i++) {
				lru.add(temp.poll());
			}
			if (first) {
				int id = buffDescr[replacementIndex].getPageNumber();
				lru.remove(id);
				mru.removeElement(id);
				// remove from loved map
				love_Hate.remove(id);
			}
			if (!found) {
				int limitS = mru.size();
				Stack<Integer> tempS = new Stack<Integer>();
				boolean firstS = false;
				for (int i = 0; i < limitS; i++) {
					int pid = mru.pop();
					int indexArr = pageToFrame.get(pid);
					if (!firstS && buffDescr[indexArr].getPin_count() == 0) {
						firstS = true;
						replacementIndex = indexArr;
					}
					tempS.add(pid);
				}
				// return again to the lru
				for (int i = 0; i < limitS; i++) {
					mru.add(tempS.pop());
				}
				if (firstS) {
					int id = buffDescr[replacementIndex].getPageNumber();
					mru.remove(id);
					// remove from map
					love_Hate.remove(id);
				}
			}

		}

		return replacementIndex;
	}

	/**
	 * Allocate new page(s). Call DB Object to allocate a run of new pages and
	 * find a frame in the buffer pool for the first page and pin it. (This call
	 * allows a client of the Buffer Manager to allocate pages on disk.) If
	 * buffer is full, i.e., you can\t find a frame for the first page, ask DB
	 * to deallocate all these pages, and return null.
	 * 
	 * @param firstPage
	 *            the address of the first page.
	 * @param howmany
	 *            total number of allocated new pages.
	 * 
	 * @return the first page id of the new pages. null, if error.
	 */

	public PageId newPage(Page firstPage, int howmany) throws ChainException {

		if (howmany < 1) {
			throw new InvalidRunSizeException(null, "Size is less than 1!!");
		}
		if (firstPage == null) {
			throw new ChainException(null, "firstPage is null");
		}

		// buffer is not full.
		if (!isBufferFull()) {

			// now allocate the page(s)
			PageId firstID = new PageId();
			try {
				SystemDefs.JavabaseDB.allocate_page(firstID, howmany);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			// now pin page to the free position
			pinPage(firstID, firstPage, false, false);

			return firstID;

		} else {

			// buffer pool is full
			// notice : no need to deallocate pages as mentioned in the handout
			// as I never allocate pages before I check that the pool has free
			// space.
			return null;

		}

		// try {
		//
		// } catch (OutOfSpaceException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// } catch (InvalidRunSizeException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// } catch (InvalidPageNumberException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// } catch (FileIOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// } catch (DiskMgrException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// } catch (IOException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }

	}

	/**
	 * This method should be called to delete a page that is on disk. This
	 * routine must call the method in diskmgr package to deallocate the page.
	 * 
	 * @param pgid
	 *            the page number in the database.
	 * @throws ChainException
	 * @throws IOException
	 */

	public void freePage(PageId pgid) throws ChainException, IOException {

		// first check if in buffer pool
		if (pageToFrame.containsKey(pgid.pid)) {
			// check its pin count
			int index = pageToFrame.get(pgid.pid);
			// no one is using it.
			if (buffDescr[index].getPin_count() < 1) {
				SystemDefs.JavabaseDB.deallocate_page(pgid);
			}
			// page pinned by more than one
			else if (buffDescr[index].getPin_count() > 1) {
				throw new PagePinnedException(null,
						"bufmgr.PagePinnedException");
			}

			// one only is using it --> first un-pin then deallocate
			else if (buffDescr[index].getPin_count() == 1) {
				unpinPage(pgid, false, false);
				SystemDefs.JavabaseDB.deallocate_page(pgid);
			}
			// more than one is using it --> throw exception

		}
		// page is no in the pool --> deallocate only
		else {

			SystemDefs.JavabaseDB.deallocate_page(pgid);
		}

	}

	public void flushAllPages() throws ChainException {
		for (int i = 0; i < bufferBusySize; i++) {
			if (buffDescr[i].isDirty()) {
				flushPage(buffDescr[i].getPageID());
			}
		}

	}

	/**
	 * Used to flush a particular page of the buffer pool to disk. This method
	 * calls the write_page method of the diskmgr package.
	 * 
	 * @param pgid
	 *            the page number in the database.
	 * @throws ChainExcetion
	 */

	public void flushPage(PageId pgid) throws ChainException {
		// 1.check if page is in pool

		if (pageToFrame.containsKey(pgid.pid)) {
			// page in pool , check if dirty --> write it
			int frameIndex = pageToFrame.get(pgid.pid);
			if (buffDescr[frameIndex].isDirty()) {
				Page pg = new Page();
				pg.setpage(buffArr[frameIndex]);

				try {
					SystemDefs.JavabaseDB.write_page(pgid, pg);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} else {
			// page is not in the pool
			throw new ChainException(null,
					"Page can't be flushed : not in the pool");
		}

	}

	public int getNumUnpinnedBuffers() {
		int counter = 0;
		// edit buffer.length instead of busy size.
		for (int i = 0; i < buffDescr.length; i++) {
			if (buffDescr[i].getPin_count() == 0)
				counter++;
		}
		// return (counter + buffDescr.length - bufferBusySize);
		return counter;
	}

	// edit : to be used by Allocate to check if there's a place in the buffer
	// pool.
	public boolean isBufferFull() {
		boolean full = true;
		for (int i = 0; i < buffDescr.length; i++) {
			if (buffDescr[i].getPin_count() == 0) {
				full = false;
				break;
			}
		}
		return full;
	}

}
