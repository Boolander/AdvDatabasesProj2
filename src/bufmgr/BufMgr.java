package bufmgr;

import global.GlobalConst;
import global.Minibase;
import global.Page;
import global.PageId;

import java.util.HashMap;

/**
 * <h3>Minibase Buffer Manager</h3>
 * The buffer manager manages an array of main memory pages.  The array is
 * called the buffer pool, each page is called a frame.  
 * It provides the following services:
 * <ol>
 * <li>Pinning and unpinning disk pages to/from frames
 * <li>Allocating and deallocating runs of disk pages and coordinating this with
 * the buffer pool
 * <li>Flushing pages from the buffer pool
 * <li>Getting relevant data
 * </ol>
 * The buffer manager is used by access methods, heap files, and
 * relational operators.
 */
public class BufMgr implements GlobalConst {

  Page[] buffer_pool;
  FrameDesc[] frametab;
  
  HashMap<Integer, Integer> page_to_frame;
  HashMap<Integer, Integer> frame_to_page;
  
  ReplacerImpl replacer;

  /**
   * Constructs a buffer manager by initializing member data.  
   * 
   * @param numframes number of frames in the buffer pool
   */
  public BufMgr(int numframes) {
  
	//initialization of buffer_pool array. This will store each 'frame'
		buffer_pool = new Page[numframes];
		frametab = new FrameDesc[numframes];
	  
	  //populates the buffer_pool and frametab arrays
		for (int i=0; i<frametab.length; i++){
 	     buffer_pool[i] = new Page();
 	     frametab[i] = new FrameDesc();
	  }
	  
	  //creates an instance of replacer and initializes hashMaps
	  replacer = new ReplacerImpl();
	  page_to_frame = new HashMap<>();
    frame_to_page = new HashMap<>();
    
  } // public BufMgr(int numframes)

  /**
   * The result of this call is that disk page number pageno should reside in
   * a frame in the buffer pool and have an additional pin assigned to it, 
   * and mempage should refer to the contents of that frame. <br><br>
   * 
   * If disk page pageno is already in the buffer pool, this simply increments 
   * the pin count.  Otherwise, this<br> 
   * <pre>
   * 	uses the replacement policy to select a frame to replace
   * 	writes the frame's contents to disk if valid and dirty
   * 	if (contents == PIN_DISKIO)
   * 		read disk page pageno into chosen frame
   * 	else (contents == PIN_MEMCPY)
   * 		copy mempage into chosen frame
   * 	[omitted from the above is maintenance of the frame table and hash map]
   * </pre>		
   * @param pageno identifies the page to pin
   * @param mempage An output parameter referring to the chosen frame.  If
   * contents==PIN_MEMCPY it is also an input parameter which is copied into
   * the chosen frame, see the contents parameter. 
   * @param contents Describes how the contents of the frame are determined.<br>  
   * If PIN_DISKIO, read the page from disk into the frame.<br>  
   * If PIN_MEMCPY, copy mempage into the frame.<br>  
   * If PIN_NOOP, copy nothing into the frame - the frame contents are irrelevant.<br>
   * Note: In the cases of PIN_MEMCPY and PIN_NOOP, disk I/O is avoided.
   * @throws IllegalArgumentException if PIN_MEMCPY and the page is pinned.
   * @throws IllegalStateException if all pages are pinned (i.e. pool is full)
   */
  public void pinPage(PageId pageno, Page mempage, int contents) {

<<<<<<< HEAD
	Integer frame_num = page_to_frame.get(pageno.pid);
	
	if (frame_num == null){
		
		//there was no frame number, so now we need to pick one
		frame_num = replacer.pickVictim();
			
		if (frame_num != -1){
      
			// Found an empty frame
			if ((frametab[frame_num].valid) && (frametab[frame_num].dirty)){
				// The frame had a page in it that became dirty,
				// so write it out to the disk before using the frame.
				Minibase.DiskManager.write_page(frametab[frame_num].pageno, buffer_pool[frame_num]);
				frametab[frame_num].dirty = false;
			}
        
			if (contents == PIN_DISKIO){
				Page disk_page = new Page();
				Minibase.DiskManager.read_page(pageno, disk_page);
				buffer_pool[frame_num].copyPage(disk_page);
				mempage.setPage(buffer_pool[frame_num]);
				frametab[frame_num].pin_count++;  
				frametab[frame_num].valid = true; 
				frametab[frame_num].dirty = false; 
				frametab[frame_num].refbit = false; 
				frametab[frame_num].pageno = new PageId(pageno.pid); 
				addToHashMap(pageno.pid, frame_num);       
			}
			else if (contents == PIN_MEMCPY){
				buffer_pool[frame_num].copyPage(mempage);  
				mempage.setPage(buffer_pool[frame_num]);
				frametab[frame_num].pin_count++;  
				frametab[frame_num].valid = true;  
				frametab[frame_num].dirty = false; 
				frametab[frame_num].refbit = false; 
				frametab[frame_num].pageno = new PageId(pageno.pid); 
				addToHashMap(pageno.pid, frame_num);    
			}
			else if (contents == PIN_NOOP){
				// No operation needed  
			}
			else{
				// Received an invalid operation
				throw new IllegalArgumentException();
			}
		}
		else{
			// Buffer pool is completely full and there are no slots that
			// can be reclaimed.  Very bad news.
			throw new IllegalStateException(); 
		}
	}
    else{
      // The page is already mapped to a frame.  Pin it and set
      // mempage to refer to it.
      frametab[frame_num].pin_count++;  
      mempage.setPage(buffer_pool[frame_num]);
    }
=======
      Integer Frame_num = page_to_frame.get(pageno.pid);
      if (Frame_num == null){
		
		  //there was no frame number, so now we need to pick one
			int frame_num = replacer.pickVictim();
            if (frame_num != -1) {
                // Found an empty frame
                if ((frametab[frame_num].valid) && (frametab[frame_num].dirty)) {
                    // The frame had a page in it that became dirty,
                    // so write it out to the disk before using the frame.
                    Minibase.DiskManager.write_page(frametab[frame_num].pageno, buffer_pool[frame_num]);
                    frametab[frame_num].dirty = false;
                }

                if (contents == PIN_DISKIO) {
                    Page disk_page = new Page();
                    Minibase.DiskManager.read_page(pageno, disk_page);
                    buffer_pool[frame_num].copyPage(disk_page);
                    mempage.setPage(buffer_pool[frame_num]);
                    frametab[frame_num].pin_count++;
                    frametab[frame_num].valid = true;
                    frametab[frame_num].dirty = false;
                    frametab[frame_num].refbit = false;
                    frametab[frame_num].pageno = new PageId(pageno.pid);
                    addToHashMap(pageno.pid, frame_num);
                } else if (contents == PIN_MEMCPY) {
                    buffer_pool[frame_num].copyPage(mempage);
                    mempage.setPage(buffer_pool[frame_num]);
                    frametab[frame_num].pin_count++;
                    frametab[frame_num].valid = true;
                    frametab[frame_num].dirty = false;
                    frametab[frame_num].refbit = false;
                    frametab[frame_num].pageno = new PageId(pageno.pid);
                    addToHashMap(pageno.pid, frame_num);
                } else if (contents == PIN_NOOP) {
                    // No operation needed
                } else {
                    // Received an invalid operation
                    throw new IllegalArgumentException();
                }

            }
                else{
                    // Buffer pool is completely full and there are no slots that
                    // can be reclaimed.  Very bad news.
                    throw new IllegalStateException();
                }
            }
      else{
        // The page is already mapped to a frame.  Pin it and set
        // mempage to refer to it.
        frametab[Frame_num].pin_count++;
        mempage.setPage(buffer_pool[Frame_num]);
      }
>>>>>>> 74b9783535aa557a5d78ce24b1882849bf40bd75
  } // public void pinPage(PageId pageno, Page page, int contents)
  
  private void addToHashMap(int page, int frame)
  {
    Integer oldPage = frame_to_page.get(frame);
    if (oldPage != null){
      page_to_frame.remove(oldPage);
      frame_to_page.remove(frame);
    }
    
    // Update both hashmaps
    page_to_frame.put(page, frame);
    frame_to_page.put(frame, page);			
		
	}

	
  //} // public void pinPage(PageId pageno, Page page, int contents)
  
  /**
   * Unpins a disk page from the buffer pool, decreasing its pin count.
   * 
   * @param pageno identifies the page to unpin
   * @param dirty UNPIN_DIRTY if the page was modified, UNPIN_CLEAN otherwise
   * @throws IllegalArgumentException if the page is not in the buffer pool
   *  or not pinned
   */
  public void unpinPage(PageId pageno, boolean dirty) {

    Integer frame_num = page_to_frame.get(pageno.pid);
    if ((frame_num == null) || frametab[frame_num].pin_count == 0){
      //Trying to unpin a page that doesn't exist
      throw new IllegalArgumentException();      
    }
    else{
      if (dirty){
        //make sure the page stays dirty until saved to disk
        frametab[frame_num].dirty = dirty;
      }
      
      // Update the pin count.
      frametab[frame_num].pin_count--;
      if (frametab[frame_num].pin_count == 0){
        // When all the pins are removed set the reference bit.
        frametab[frame_num].refbit = true;
      }
    }
    
    

  } // public void unpinPage(PageId pageno, boolean dirty)
  
  /**
   * Allocates a run of new disk pages and pins the first one in the buffer pool.
   * The pin will be made using PIN_MEMCPY.  Watch out for disk page leaks.
   * 
   * @param firstpg input and output: holds the contents of the first allocated page
   * and refers to the frame where it resides
   * @param run_size input: number of pages to allocate
   * @return page id of the first allocated page
   * @throws IllegalArgumentException if firstpg is already pinned
   * @throws IllegalStateException if all pages are pinned (i.e. pool exceeded)
   */
  public PageId newPage(Page first_page, int run_size) {

    if (getNumUnpinned() == 0){
      //Everything is already unpinned, to the pool is clear
    	throw new IllegalStateException();      
    }
    else{
    	PageId pageno = Minibase.DiskManager.allocate_page(run_size);
    	
    	Integer frame_num = page_to_frame.get(pageno.pid);
    	
    	if ((frame_num != null) && (frametab[frame_num].pin_count > 0)){
        // The first page is already mapped into the buffer pool and pinned
        throw new IllegalArgumentException(); 
      }
      else{
        // Pin the first page and return its page id
        pinPage(pageno, first_page, PIN_MEMCPY);
        return pageno;
      }
    }
  }
    
 // public PageId newPage(Page firstpg, int run_size)

  /**
   * Deallocates a single page from disk, freeing it from the pool if needed.
   * 
   * @param pageno identifies the page to remove
   * @throws IllegalArgumentException if the page is pinned
   */
  public void freePage(PageId pageno) {

    Integer frame_num = page_to_frame.get(pageno.pid);
    
    if (frame_num != null && frametab[frame_num].pin_count >0){
    	throw new IllegalArgumentException();
    }
    else{
    	Minibase.DiskManager.deallocate_page(pageno);
    }
    
  } // public void freePage(PageId firstid)

  /**
   * Write all valid and dirty frames to disk.
   * Note flushing involves only writing, not unpinning or freeing
   * or the like.
   * 
   */
  public void flushAllPages() {

    for (int i = 0; i < frametab.length; i++){
    
    	if ((frametab[i].dirty == true) && (frametab[i].valid == true)){
    
    		flushPage(frametab[i].pageno);
    		frametab[i].dirty = false;
    	}
    }

  } // public void flushAllFrames()

  /**
   * Write a page in the buffer pool to disk, if dirty.
   * 
   * @throws IllegalArgumentException if the page is not in the buffer pool
   */
  public void flushPage(PageId pageno) {
	  	
		Integer frame_num = page_to_frame.get(pageno.pid);
	
		if(frame_num != null){
			Minibase.DiskManager.write_page(pageno, buffer_pool[frame_num]);
		}
		else{
			throw new IllegalArgumentException();  
		}    
  }

   /**
   * Gets the total number of buffer frames.
   */
  public int getNumBuffers() {
    return frametab.length;
  }

  /**
   * Gets the total number of unpinned buffer frames.
   */
  public int getNumUnpinned() {
    int total_unpinned = 0;
    
    for (int i = 0; i < frametab.length; i++){
      if (frametab[i].pin_count == 0){
      	total_unpinned++;
      }
    } 
    return total_unpinned;
  }

} // public class BufMgr implements GlobalConst
