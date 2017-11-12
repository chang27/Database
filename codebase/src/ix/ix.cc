
#include <stack>
#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}
RC IndexManager::createFile(const string &fileName)
{
    PagedFileManager *pfm = PagedFileManager::instance();
    RC rc = pfm->createFile(fileName);
    return rc;
}

RC IndexManager::destroyFile(const string &fileName)
{
    PagedFileManager *pfm = PagedFileManager::instance();
    RC rc = pfm->destroyFile(fileName);
    return rc;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    //check again how the fileHandle open and close
	if(ixfileHandle.alreadyOpen()) return -1;
	PagedFileManager *pfm = PagedFileManager::instance();
	RC rc = pfm->openFile(fileName,ixfileHandle.fileHandle);

	return rc;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	if(! ixfileHandle.alreadyOpen()) return -1;
	PagedFileManager *pfm = PagedFileManager::instance();
	RC rc = pfm->closeFile(ixfileHandle.fileHandle);

    return rc;
}
RC initialMD(FileHandle &fileHandle) {
	void *buffer = malloc(PAGE_SIZE);
	memset(buffer, 1, sizeof(short));
	RC rc = fileHandle.appendPage(buffer);
	free(buffer);
	return rc;
}

RC firstLeaf(IXFileHandle &ixfileHandle, const Attribute &attribute,const void *key, unsigned int &keyLen, const RID &rid){
	if(ixfileHandle.fileHandle.getNumberOfPages() > 0) return -1;
	initialMD(ixfileHandle.fileHandle);
	void *root = malloc(PAGE_SIZE);
	memset(root, -1, 2);//set ParentNum to -1
	memset((char *)root+2, 1, 2);//total keys == 1(short)
	memset((char *)root+4, 1, 2); //is leaf
	memset((char *)root+6, -1, 2);//next is empty

	//set the first key entry in the root
	keyLen = attribute.type==TypeVarChar ? *((int *) key) + 4 : 4;

	memcpy((char *)root+8, key, keyLen);
	memcpy((char *)root+8+keyLen, &rid.pageNum, 4);
	memcpy((char *)root+12+keyLen, &rid.slotNum, 4);

	short freeSpaceOffset = 16 + keyLen;
	memcpy((char *)root+PAGE_SIZE-2, &freeSpaceOffset, 2);
	RC rc = ixfileHandle.fileHandle.appendPage(root);

	free(root);
	return rc;
}

short getRootPage(FileHandle & fileHandle) {

	void *buffer = malloc(PAGE_SIZE);
	RC rc = fileHandle.readPage(0, buffer);
	if(rc < 0) {
		free(buffer);
		return -1;
	}
	short pageNum = *(short *)buffer;
	free(buffer);
	return pageNum;
}

void getDirectory(void *page, short &parentPageNum, short &totalKeys, short &isLeaf, short &rightSibling, short &freeSpaceOffset, bool & doubled){
	parentPageNum = *(short *)page;
	totalKeys = *(short *)((char *)page+2);
	isLeaf = *(short *)((char *)page+4);
	rightSibling = *(short *)((char *)page+6);
	if(doubled){
		freeSpaceOffset = *(short *)((char *)page + PAGE_SIZE + PAGE_SIZE-2);
	}else{
		freeSpaceOffset = *(short *)((char *)page +PAGE_SIZE-2);
	}

}

void writeDirectory(void *page, const short & parentPageNum, const short & totalKeys, const short & isLeaf, const short & rightSibling, const short &freeSpaceOffset) {

	memcpy(page, &parentPageNum, 2);
	memcpy((char *)page + 2, &totalKeys, 2);
	memcpy((char *)page + 4, &isLeaf, 2);
	memcpy((char *)page + 6, &rightSibling, 2);
	memcpy((char *)page + PAGE_SIZE - 2, &freeSpaceOffset, 2);

}

// that is to compare the key with the currentValues in the given page with proper offset:
int compareWithKey(const void *currentPage, const short &offset, const void *key, const Attribute &attribute){
	// -1:key is larger
	// 0: key is equal
	// 1 :key is smaller, this is the right place
	switch(attribute.type){
	case TypeInt:
		int keyValue = *(int *)key;
		int currentValue = *(int *)((char *)currentPage + offset);
		if(keyValue < currentValue){
			return 1;
		}
		else if(keyValue == currentValue ){
			//	isExist = true;
				return 0;
		}else{
			return -1;
		}

	case TypeReal:
		float keyValue1 = *(float *)key;
		float currentValue1 = *(float *)((char *)currentPage + offset);
		if(keyValue1 < currentValue1){
			return 1;
		}else if(keyValue1 == currentValue1){
		//		isExist = true;
				return 0;
		}else{
			return -1;
		}
	case TypeVarChar:
		int KeyLen = *(int *)key;
		int currentLen = *(int *)((char *)currentPage+offset);
		string keyValue2;
		for (int i=0;i<KeyLen; i++) keyValue2.push_back(*(char *)((char *)key+i));
		string currentValue2;
		for (int i=0; i<currentLen; i++) currentValue2.push_back(*(char *)((char *)currentPage+offset+i));
		if(strcmp(keyValue2.c_str(),currentValue2.c_str())==0){
		//	isExist=true;
			return 0;
		}
		return strcmp(currentValue2.c_str(),keyValue2.c_str());
	}
	return -2;
}
//check this again, can this be used in index-index-index-leaf all?
int getKeyOffsetInParent(void *parentNode, const short &totalKeys, const void *key,const Attribute &attribute ){
	//return the offset, which is the position of the child, in which we want to insert key
	int res = -1;
	int result = -1;
	//int keyLen = attribute.type==TypeVarChar ? *((int *) key)+4 : 4;
	int i=0;
	short offset = 10;
	for(; i< totalKeys; i++){
		res = compareWithKey(parentNode, offset, key, attribute);
		if(res == -2) return -1;
		if(res>0){  // key should be inserted into left child.
			result = *(short *)((char *)parentNode + offset - 2);
			break;
		}else{
			int len = attribute.type == TypeVarChar ? *(int *)((char *)parentNode + offset) + 4 : 4;
			offset += (2 + len);
			//continue;
		}
	}
	if(i==totalKeys){
		result = *(short *)((char *)parentNode + offset - 2);
		//we searched through all the entries and insert into right child of last key;

//		short freeSpaceStart = *(short *)((char *)parentNode+PAGE_SIZE-2);
//		if(freeSpaceStart != offset) return -1;
	}
//	offset -= 2;//so now the offset is for the page number of child
//	if(equal){
//		offset += 2 + keyLen;
//	}
	return result;
}


RC getKeyOffsetinLeaf(){

	return 0;
}
RC getLeftNode(FileHandle & fileHandle, const Attribute &attribute, const void *key, stack<short> &internalStack){
	if(! fileHandle.alreadyOpen()){
		return -1;
	}
	short rootPage = getRootPage(fileHandle);
	if(rootPage < 0) return -1;
	void *buffer;
	//RC rc = fileHandle.readPage(rootPage, buffer);
	//short isLeaf = *(short *)((char *)buffer + 4);
	while(true){
		internalStack.push(rootPage);
		buffer = malloc(PAGE_SIZE);
		RC rc = fileHandle.readPage(rootPage, buffer);
		if(rc < 0) return -1;
		short isLeaf = *(short *)((char *)buffer + 4);
		if(isLeaf){
			break;
		}
		short totalKeys = *(short *)((char *)buffer + 2);
		rootPage = getKeyOffsetInParent(buffer, totalKeys, key, attribute);
		free(buffer);
		if(rootPage == -1) return -1;
//		int i = 0;
//		int offset = 8 + 2;
//		for(; i < totalKeys; i++){
//			boolean
//			if(compareWithKey(rootPage, offset, key, attribute))
//		}
	}
	//find the parent and the leaf
	return 0;
}

RC splitInternalNode(void* buffer, const Attribute & attribute, void *upperKey, const short &c_pageNum, short & r_pageNum, FileHandle &fileHandle){
	void * newPage = malloc(PAGE_SIZE);
	short isRoot, totalKeys, isLeaf, nextPage, offset;
	bool doubled = true;
	getDirectory(buffer, isRoot, totalKeys, isLeaf, nextPage, offset, doubled);
	int start = 10;
	int keyLen = -1;
	short keyNum = totalKeys/2;
	for(int i = 0; i < keyNum; i++) {
		keyLen = attribute.type == TypeVarChar ? *(int *)((char *)buffer + start) + 4 : 4;
		start += (keyLen + 2);
	}
	keyLen = attribute.type == TypeVarChar ? *(int *)((char *)buffer + start) + 4 : 4;

	memcpy(upperKey, (char *)buffer + start, keyLen); // this is to record the key to be transferred to upper level;
	start += keyLen;
	memmove((char *)newPage + 8, (char *)buffer + start, offset - start);
	if(isRoot == -1){
		isRoot = 0;
	}
	short r_keyNum = totalKeys - keyNum - 1;
	short r_offset = offset - start + 8;
	writeDirectory(newPage, isRoot, r_keyNum, isLeaf, nextPage, r_offset);
	fileHandle.appendPage(newPage);
	r_pageNum = fileHandle.getNumberOfPages() - 1;
	start -= keyLen;
	writeDirectory(buffer, isRoot, keyNum, isLeaf, r_pageNum, start);
	fileHandle.writePage(c_pageNum, buffer);
	free(newPage);
	return 0;
}

RC buildRoot(void *page, const Attribute& attribute, void *key, const short &leftLeaf, const short &rightLeaf){
	short isRoot = -1, nextPage = -1, totalKeys = 1, isLeaf = 0;
	int keyLen =  attribute.type == TypeVarChar ? *(int *)((char *)key) + 4 : 4;
	short offset = 8 + keyLen + 4;
	writeDirectory(page, isRoot, totalKeys, isLeaf, nextPage, offset);
	memcpy((char *)page + 8, &leftLeaf, 2);
	memcpy((char *)page + 10, key, keyLen);
	memcpy((char *)page + 10 + keyLen, &rightLeaf, 2);
	return 0;
}

RC insertInternalNode(FileHandle & fileHandle, const Attribute &attribute, short pageNum, short leftLeaf, short rightLeaf, void *key, stack<short> &parentStack) {
	if(!fileHandle.alreadyOpen()){
		return -1;
	}
	if(pageNum == -1){
		void *page = malloc(PAGE_SIZE);
		buildRoot(page, attribute,key, leftLeaf, rightLeaf);
		fileHandle.appendPage(page);
		free(page);
		short newRoot = fileHandle.getNumberOfPages() - 1;
		void *md = malloc(PAGE_SIZE);
		memcpy(md, &newRoot, 2);
		RC rc = fileHandle.writePage(0, md);
		free(md);
		return rc;
	}
	void *buffer = malloc(PAGE_SIZE + PAGE_SIZE);
	fileHandle.readPage(pageNum, buffer);
	short isRoot, totalKeys, isLeaf, nextPage, offset;
	bool doubled = false;
	getDirectory(buffer, isRoot, totalKeys, isLeaf, nextPage, offset, doubled);
	/***********need check *******/
	int parent = -1; // if this page is root;
	if(! parentStack.empty()){
		parent = parentStack.top();
		parentStack.pop();
	}
	bool split = false;
	int recordLen = attribute.type == TypeVarChar ? *(int *)((char *)key) + 4 : 4;
	if(offset + recordLen + 2 + 2 > PAGE_SIZE) {
		split = true;
	}
	int start = 10;
	int i = 0;
	int res = -1;
	for(; i< totalKeys; i++){
		res = compareWithKey(buffer, start, key, attribute);
		if(res == -2) return -1;
		if(res>0){  // key should be inserted into left child.
			//	result = *(short *)((char *)buffer + start - 2);
			break;
		}else{
				int len = attribute.type == TypeVarChar ? *(int *)((char *)buffer + start) + 4 : 4;
				start += (2 + len);
				//continue;
		}
	}

	memmove((char *)buffer + start + recordLen + 2, (char *)buffer + start, offset - start);
	memcpy((char *)buffer + start, key, recordLen);
	memcpy((char *)buffer + start + recordLen, &rightLeaf, 2);
	totalKeys++;
	offset += (2 + recordLen);
	memcpy((char *)buffer + 2, &totalKeys, 2);
	if(split) {
		memcpy((char *)buffer + PAGE_SIZE + PAGE_SIZE - 2, &offset, 2);
		short r_pageNum;
		void *upperKey = malloc(52);
		//void *newPage = malloc(PAGE_SIZE);
		splitInternalNode(buffer, attribute, upperKey, pageNum, r_pageNum, fileHandle);
		free(buffer);
		insertInternalNode(fileHandle, attribute, parent, pageNum, r_pageNum, upperKey, parentStack);
		free(upperKey);
	}
	else{
		memcpy((char *)buffer + PAGE_SIZE - 2, &offset, 2);
		fileHandle.writePage(pageNum, buffer);
		free(buffer);
	}

	return 0;

}

RC insertDuplicate(FileHandle &fileHandle,
		const Attribute &attribute,
		void *buffer, const short &offset, const short &pageNum,
		const void *key, const short &keyLen, const RID &rid){
	//int duplicateEntryLen  = attribute.type==TypeVarChar ? *(int *)((char *)buffer + offset)+4 : 4; //same as keyLen
	short duplicatePageNum = *(short *)((char *)buffer+offset+keyLen);
	short duplicateSlotNum = *(short *)((char *)buffer+offset+keyLen+2);
	if(duplicatePageNum == -1){
	//there is some overflow page for this key
		short overflowPageNum = duplicateSlotNum;
		void *overflowPage = malloc(PAGE_SIZE);
		RC rc1 = fileHandle.readPage(overflowPageNum, overflowPage);
		short freeSpaceStartAtOP = *(short *)((char *)overflowPage+PAGE_SIZE - 4);
		if(freeSpaceStartAtOP + 2*2 + 4 <= PAGE_SIZE){//the first overflow page is not full
			//insert to this page
			memcpy((char *)overflowPage + freeSpaceStartAtOP, &rid , 4);
			//nothing for the leaf page to be changed, but to update the directory of overflow page
			freeSpaceStartAtOP += 4;
			memcpy((char *)overflowPage + PAGE_SIZE -4,&freeSpaceStartAtOP, 2);
			RC rc = fileHandle.writePage(overflowPageNum,overflowPage);
			free(buffer);
			free(overflowPage);
			return rc;
		}else{//the first overflow page is full
			short nextOFPNum = *(short *)((char *)overflowPage +PAGE_SIZE-2);
			while(nextOFPNum != -1){
			//read next overflow page and check for insertion
				fileHandle.readPage(nextOFPNum, overflowPage);
				freeSpaceStartAtOP = *(short *)((char *)overflowPage + PAGE_SIZE - 4);
				if(freeSpaceStartAtOP + 2*2 + 4 <= PAGE_SIZE){
					break;
				}
				nextOFPNum = *(short *)((char *)overflowPage +PAGE_SIZE-2);
			}


			if(nextOFPNum == -1){
				//the last overflow page is full, we need a new overflow page
				void *newOverflowPage = malloc(PAGE_SIZE);
				memcpy(newOverflowPage,&rid, 4);
				memset((char *)newOverflowPage+PAGE_SIZE-2,-1,2);//next pointer
				memset((char *)newOverflowPage+PAGE_SIZE-4, 4, 2);//free space start offset
				RC rc1 = fileHandle.appendPage(newOverflowPage);
				free(buffer);
				free(overflowPage);
				free(newOverflowPage);
				return rc1;

			} else{
				//the current overflow page is not full, we can insert to it
				memcpy((char *)overflowPage+freeSpaceStartAtOP, &rid, 4);
				freeSpaceStartAtOP += 4;
				memcpy((char *)overflowPage+PAGE_SIZE -4, &freeSpaceStartAtOP, 2);
				RC rc2 = fileHandle.writePage(nextOFPNum,overflowPage);
				free(buffer);
				free(overflowPage);
				return rc2;
			}
		}
	}else{
		//there is no overflow page, we need create one
		void *firstOverflowPage = malloc(PAGE_SIZE);
		memcpy(firstOverflowPage,&duplicatePageNum, 2);
		memcpy((char *)firstOverflowPage+2, &duplicateSlotNum, 2);
		memcpy((char *)firstOverflowPage+4, &rid, 4);
		memset((char *)firstOverflowPage+PAGE_SIZE-2,8,2);

		fileHandle.appendPage(firstOverflowPage);

				//update the slot in leaf
		memset((char *)buffer+offset+keyLen, -1, 2);
		short firstOverflowPageNum = fileHandle.getNumberOfPages()-1;
		memcpy((char *)buffer+offset+keyLen+2, &firstOverflowPageNum,2);

		RC rc3 = fileHandle.writePage(pageNum,buffer);
		free(buffer);
		free(firstOverflowPage);
		return rc3;
	}
}
//one thing is missing :parent stack!
RC insertToLeaf(FileHandle &fileHandle,
		const Attribute &attribute,
		const int & pageNum,//the page number of the leaf
		const void* key,
		const RID &rid,
		){
	void *buffer = malloc(PAGE_SIZE);
	RC rc = fileHandle.readPage(pageNum,buffer);
	if(rc<0) {
		free(buffer);
		return -1;
	}
	short parentNum, totalEntries, isLeaf, rightSibling,freeSpaceOffset;
	bool doubled = false;
	getDirectory(buffer,parentNum, totalEntries, isLeaf, rightSibling, freeSpaceOffset, doubled);
	int keyLen = attribute.type==TypeVarChar ?  *(int *)((char *)key) + 4 : 4;
	short offset = 8;
	bool isExist = false;
	//find the offset, which is the right place to insert the key
	for (int i=0; i<totalEntries; i++){
		int res= compareWithKey(buffer, offset, key, attribute);
		if(res>=0){
			if(res==0) isExist=true;
			break;
		}else {
			int len = attribute.type==TypeVarChar ? *(int *)((char *)buffer + offset)+4+4 : 4 + 4;
			offset += len;

		}
	}
	if(isExist){
		//duplicate operation is the same, whether the leaf is full or not
		RC rc5 = insertDuplicate(fileHandle, attribute, buffer, offset,pageNum, key, keyLen, rid);
		return rc5;
	}
	if(freeSpaceOffset + keyLen + 2*2 +2<=PAGE_SIZE){ //the leaf is not full, insert directly
			//normal insertion
			memmove((char *)buffer+offset+keyLen+4, (char *)buffer+offset, freeSpaceOffset-offset);
			memcpy((char *)buffer+offset, key, keyLen);
			memcpy((char *)buffer+offset+keyLen, &rid.pageNum, 2);
			memcpy((char *)buffer+offset+keyLen+2, &rid.slotNum, 2);

			totalEntries += 1;
			memcpy((char *)buffer+2, &totalEntries, 2);
			freeSpaceOffset += keyLen+4;
			memcpy((char *)buffer+PAGE_SIZE-2, &freeSpaceOffset, 2);

			RC rc1 = fileHandle.writePage(pageNum,buffer);
			free(buffer);
			return rc1;
	}else{
		//split is needed
		void *tempPage = malloc(2*PAGE_SIZE);
		void *rightPage = malloc(PAGE_SIZE);

		memcpy((char *)tempPage + 2*PAGE_SIZE -2, &freeSpaceOffset, 2);
		memmove(tempPage, buffer, freeSpaceOffset);

		memmove((char *)tempPage+offset+keyLen+4, (char *)tempPage+offset, freeSpaceOffset-offset);
		memcpy((char *)tempPage+offset, key, keyLen);
		memcpy((char *)tempPage+offset+keyLen, &rid.pageNum, 2);
		memcpy((char *)tempPage+offset+keyLen+2, &rid.slotNum, 2);

		freeSpaceOffset += keyLen+4;
		totalEntries += 1;
		short midPoint = 8;
		short entryLen = 4;

		for (int i=0; i<totalEntries/2; i++){
			if(attribute.type == TypeVarChar){
				entryLen = *(int *)((char *)tempPage +midPoint)+ 4;
			}
			midPoint += entryLen + 4;
		}
		short rightEntriesNum = totalEntries - totalEntries/2;


		//set up right page's directory & entries, append it
		memcpy(rightPage, &parentNum, 2);
		memcpy((char *)rightPage+2, &rightEntriesNum, 2);
		memcpy((char *)rightPage+4, &isLeaf, 2);
		memcpy((char *)rightPage+6, &rightSibling, 2);
		short rightFreeSpaceOffset = freeSpaceOffset - midPoint + 8;
		memcpy((char *)right+PAGE_SIZE-2,&rightFreeSpaceOffset, 2);

		memmove((char *)rightPage + 8, (char *)tempPage+midPoint,freeSpaceOffset-midPoint);
		RC rc2 = fileHandle.appendPage(rightPage);

		if(rc2<0) return rc2;
		//find the entry to lift

		short midEntryLen  =  attribute.type==TypeVarChar ? *(int *)((char *)rightPage + 8)+4 :4 ;
		void *midEntry = malloc(midEntryLen);
		memcpy(midEntry, (char *)rightPage +8, midEntryLen);
		//update left leaf page
		totalEntries /= 2;
		memcpy(buffer, &parentNum, 2);
		memcpy((char *)buffer+2, &totalEntries, 2);
		memcpy((char *)buffer+4, &isLeaf, 2);
		short newRightSibling = fileHandle.getNumberOfPages()-1;
		memcpy((char *)buffer+6, &newRightSibling, 2);
		memcpy((char *)buffer+PAGE_SIZE-2, &midPoint, 2);

		memcpy((char *)buffer + 8, (char *)tempPage + 8, midPoint-8);
		RC rc3 = fileHandle.writePage(pageNum,buffer);

		//push the mid entry to the parent node, update the index:
		//short parentPageNum = getParent();//need to implement
		//RC rc4 = insertToIndex();//need to implement
		free(buffer);
		free(tempPage);
		free(rightPage);
		free(midEntry);
		return rc3;
	}
}
RC findMidPoint(){
	return 0;
}
RC splitLeaf(){
	return 0;
}
RC insertToInner(){
	return 0;
}
RC splitInner(){
	return 0;
}


RC IndexManager::insertEntry(IXFileHandle &ixfileHandle,
		const Attribute &attribute,
		const void *key,
		const RID &rid)
{
	FileHandle fileHandle = ixfileHandle.fileHandle;
	if(! fileHandle.alreadyOpen()) return -1;


	int pageNum = fileHandle.getNumberOfPages();
	unsigned int keyLen=0;
	RC rc;
	if(pageNum==0){
		//if the number of pages is 0: build a root(which is also a leaf)
		//while building the root, also insert the key
		rc = firstLeaf(ixfileHandle, attribute, key, keyLen, rid);

		//and copy to the page directly
	}else{
		//RC rc2 = insertToLeaf();
		//return rc2;
		//there is no need for parent, just insert at this page and check for split
		stack<short>internalStack;
		RC rc2 = getLeftNode(fileHandle, attribute, key, internalStack);
		if(rc2 < 0) return rc2;
		short leafPage = internalStack.empty() ? 1 : internalStack.top();
		internalStack.pop();
		rc = insertToLeaf();

	} //else{
	//	getParent();
		//insertToLeaf();
		//else:find the parent and the leaf node for insertion
	//}
	//insert to leaf node->
		//if the insertion doesn't need split:just insert
		//else:split the leaf node and push the middle entry into its parent
			//if the parent doesn't need to split: just insert into the node
			//else:split the index node and push the middle entry into its parent, recurse on inserting to parent index node

	return rc;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return -1;
}

RC IX_ScanIterator::close()
{
    return -1;
}


IXFileHandle::IXFileHandle()
{
	fileHandle = FileHandle();
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}
bool IXFileHandle::alreadyOpen(){
	return fileHandle.alreadyOpen();
}
RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    //how & where are the counters udpated?
	ixReadPageCounter = fileHandle.readPageCounter();
    ixWritePageCounter = fileHandle.writePageCounter();
    ixAppendPageCounter = fileHandle.appendPageCounter();

    readPageCount = ixReadPageCounter;
    writePageCount = ixWritePageCounter;
    appendPageCount = ixAppendPageCounter;

	return 0;
}

