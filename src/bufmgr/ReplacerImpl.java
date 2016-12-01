package bufmgr;

import bufmgr.Replacer;

/**
 * Implementation class for buffer pool replacement policies.  The purpose of this class is
 * to find a valid spot in the frametab array for page data to be entered.  
 * 
 * @author Geoffrey Johnson
 */

class ReplacerImpl extends Replacer {
	
	int counter;
	/**
	 * Constructor
	 * 
	 * @param bufmgr
	 */
	protected ReplacerImpl(BufMgr bufmgr) {
		this.frametab = bufmgr.frametab;
		counter = 0;
    }
	
	/**
	 * pinPage
	 * 
	 * Geoff, I need you to explain this.  All this does is call pickVictim and assign its
	 * return to a variable that disappears as soon as the function is finished...
	 * 
	 * @param fdesc
	 */
	public void pinPage(FrameDesc fdesc){
		int framenum = pickVictim();
	}
	
	/**
	 * pickVictim
	 * 
	 * The pickVictim class iterates through the frametab array, looking for a 
	 * valid place to to insert the page data.  If no valid spot is found, return -1
	 * and force an error.
	 */
	public int pickVictim(){
		int return_value = -1;
		
		for( int current = 0; current < (frametab.length*2); current++ ){
		
			
			if (!frametab[counter].valid){			
				return_value = counter;					
			}
				
			else if(frametab[counter].pin_count == 0){
			
				if(frametab[counter].refbit){
				
					frametab[counter].refbit = false;
				}
				else {       //valid frametab found, set return value to it and break the loop
				
				return_value = counter;
				break;
				}
			}
			
			counter = (counter + 1)%frametab.length; 
		}
		
		
		return return_value;
	}
	/**
	 * Currently the three functions below are not used, but they are a 
	 * part of the base class that this class extends, so they are included 
	 * here for possible later use. 
	 */
	public void unpinPage(FrameDesc fdesc){}
	public void freePage(FrameDesc fdesc){}
	public void newPage(FrameDesc fdesc){}
}