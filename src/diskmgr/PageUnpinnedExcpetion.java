package diskmgr;

import chainexception.ChainException;

public class PageUnpinnedExcpetion extends ChainException {
	
	public PageUnpinnedExcpetion(Exception e, String name)
    { 
      super(e, name); 
    }

}
