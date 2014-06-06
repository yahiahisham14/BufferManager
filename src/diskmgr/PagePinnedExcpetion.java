package diskmgr;

import chainexception.ChainException;

public class PagePinnedExcpetion extends ChainException {
	
	public PagePinnedExcpetion(Exception e, String name)
    { 
      super(e, name); 
    }

}
