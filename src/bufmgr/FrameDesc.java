package bufmgr;

import global.GlobalConst;
import global.Minibase;
import global.Page;
import global.PageId;

import java.util.HashMap;

//FrameDesc stores data for each indivicual frame. This data is external to the Page class.

public class FrameDesc{
	
	protected boolean dirty;
    protected int pin_count;
    protected boolean valid;
    protected boolean refbit;
    protected PageId pageno;
	
	public FrameDesc(){
		
        dirty = false;
		valid = false;
        pageno = null;
        pin_count = 0;
        refbit = false;

	}

}