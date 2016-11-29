package bufmgr;

import bufmgr.Replacer;

/**
 * Implementation class for buffer pool replacement policies.
 */

class ReplacerImpl extends Replacer {
	
	protected ReplacerImpl(BufMgr bufmgr) {
		this.frametab = bufmgr.frametab;
    }
	
	public void pinPage(FrameDesc fdesc){
		int framenum = pickVictim();
	}
	
	public int pickVictim(){
		
		int return_value = -1;
		
		for( int current = 0; current < frametab.length; current++ )
		{
			if(frametab[current].pin_count == 0)
			{
				if(frametab[current].refbit)
				{
					frametab[current].refbit = false;
				}
				return_value = current;
			}
		}
		
		return return_value;
	}
	
	public void unpinPage(FrameDesc fdesc){
		
	}
	
	public void freePage(FrameDesc fdesc){
		
	}
	
	public void newPage(FrameDesc fdesc){
		
	}
}