package bufmgr;

import global.PageId;

/**
 * The FrameDesc class contains fields that describe the states of individual frames.  
 * This data is external to the Page class.  -  Stefan  Is this line necessary?
 * 
 * @author Stefan Gurgurich
 */
public class FrameDesc{

	//Whether the page has been modified since it was brought into the buffer pool
	protected boolean dirty;
	//The number of times that the page currently in a given frame has 
	//been requested but not released
    protected int pin_count;
    //Whether the frame contains data which reflects the data in a disk page
    protected boolean valid;
    //Reference bit for keeping track of page access
    protected boolean refbit;
    //Disk Page Number
    protected PageId pageno;
	
	public FrameDesc(){
		
        dirty = false;
		valid = false;
        pageno = null;
        pin_count = 0;
        refbit = false;

	}

}