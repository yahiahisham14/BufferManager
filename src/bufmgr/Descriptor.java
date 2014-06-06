package bufmgr;
import global.PageId;

public class Descriptor {
	private  PageId pageNumber=null;
	private int pin_count;
	private boolean dirty;

	public Descriptor() {
		pageNumber=new PageId();
		pageNumber.pid=-1;
		this.pin_count=0;
		this.dirty=false;
	}

	public int getPin_count() {
		return pin_count;
	}

	public void setPin_count(int pin_count) {
		this.pin_count = pin_count;
	}

	public boolean isDirty() {
		return dirty;
	}

	public void setDirty(boolean dirty) {
		this.dirty = dirty;
	}

	public int getPageNumber() {
		return pageNumber.pid;
	}

	public void setPageNumber(Integer pageNumber) {
		this.pageNumber.pid = pageNumber;
	}
	
	public PageId getPageID(){
		return this.pageNumber;
	}

	
}
