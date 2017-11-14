
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
	short flag = 1;
	memcpy(buffer, &flag, sizeof(short)); //Meta data page to indicate the root pageNum using first short.
	RC rc = fileHandle.appendPage(buffer);
	free(buffer);
	return rc;
}

RC firstLeaf(IXFileHandle &ixfileHandle, const Attribute &attribute,const void *key, unsigned int &keyLen, const RID &rid){
	if(ixfileHandle.fileHandle.getNumberOfPages() > 0) return -1;
	initialMD(ixfileHandle.fileHandle);
	void *root = malloc(PAGE_SIZE);
	short flag = -1;
	memcpy(root, &flag, 2);//set ParentNum to -1
	flag = 1;
	memcpy((char *)root+2, &flag, 2);//total keys == 1(short)
	flag = 1;
	memcpy((char *)root+4, &flag, 2); //is leaf
	flag = -1;
	memcpy((char *)root+6, &flag, 2);//next is empty

	//set the first key entry in the root
	keyLen = attribute.type==TypeVarChar ? *(int *)((char *) key) + 4 : 4;

	memcpy((char *)root+8, key, keyLen);
	memcpy((char *)root+8+keyLen, &rid.pageNum, 2);
	memcpy((char *)root+10+keyLen, &rid.slotNum, 2);

	short freeSpaceOffset = 12 + keyLen;
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
	//cout<<"the root page num is "<<pageNum<<endl;
	free(buffer);
	return pageNum;
}

void getDirectory(void *page, short &parent, short &totalKeys, short &isLeaf, short &rightSibling, short &freeSpaceOffset, bool & doubled){
	parent = *(short *)page;
	totalKeys = *(short *)((char *)page+2);
	isLeaf = *(short *)((char *)page+4);
	rightSibling = *(short *)((char *)page+6);
	if(doubled){
		freeSpaceOffset = *(short *)((char *)page + PAGE_SIZE + PAGE_SIZE-2);
	}else{
		freeSpaceOffset = *(short *)((char *)page +PAGE_SIZE-2);
	}

}

void writeDirectory(void *page, const short & parent, const short & totalKeys, const short & isLeaf, const short & rightSibling, const short &freeSpaceOffset) {

	memcpy(page, &parent, 2);
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
	int keyValue, currentValue;
	float keyValue1, currentValue1;
	int keyLen, currentLen;

	switch(attribute.type){
	case TypeInt:
		keyValue = *(int *)key;
		currentValue = *(int *)((char *)currentPage + offset);
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
		keyValue1 = *(float *)key;
		currentValue1 = *(float *)((char *)currentPage + offset);
		if(keyValue1 < currentValue1){
			return 1;
		}else if(keyValue1 == currentValue1){
		//		isExist = true;
				return 0;
		}else{
			return -1;
		}
	case TypeVarChar:
		keyLen = *(int *)key;
		currentLen = *(int *)((char *)currentPage+offset);
		string keyValue2;
		for (int i=0;i<keyLen; i++) keyValue2.push_back(*(char *)((char *)key+i + 4));
		string currentValue2;
		for (int i=0; i<currentLen; i++) currentValue2.push_back(*(char *)((char *)currentPage+offset+i + 4));
		if(strcmp(keyValue2.c_str(),currentValue2.c_str())==0){
		//	isExist=true;
			return 0;
		}
		return strcmp(currentValue2.c_str(),keyValue2.c_str());
	}
	return -2;
}



int getKeyOffsetInParent(void *parentNode, const short &totalKeys, const void *key, const Attribute &attribute ){
	//return the offset, which is the position of the child, in which we want to insert key
	int res = -1;
	int result = -1;
	//int keyLen = attribute.type==TypeVarChar ? *((int *) key)+4 : 4;
	int i=0;
	short offset = 10;
	for(; i< totalKeys; i++){
		res = compareWithKey(parentNode, offset, key, attribute);
		if(res == -2) return -1;
		if(res>0){  // key should be inserted into left child, key is smaller than the parent offset:

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


RC getLeafNode(FileHandle & fileHandle, const Attribute &attribute, const void *key, stack<short> &internalStack){
	if(!fileHandle.alreadyOpen()){
		return -1;
	}
	short rootPage = getRootPage(fileHandle);
	if(rootPage < 0) return -1;
	void *buffer;
	buffer = malloc(PAGE_SIZE);
	//RC rc = fileHandle.readPage(rootPage, buffer);
	//short isLeaf = *(short *)((char *)buffer + 4);
	//while(true){
	int count = 0;
		while(count <5){
		internalStack.push(rootPage);
		//cout<<"rootPage"<<rootPage<<endl;
		RC rc = fileHandle.readPage(rootPage, buffer);

		if(rc < 0) {
			//internalStack.clear();
			free(buffer);
			return -1;
		}

		short isLeaf = *(short *)((char *)buffer + 4);
		//cout<<"is leaf flag"<<isLeaf<<endl;
		if(isLeaf){
			break;
		}
		short totalKeys = *(short *)((char *)buffer + 2);
		rootPage = getKeyOffsetInParent(buffer, totalKeys, key, attribute);
		//cout<<"new page number is"<<rootPage<<endl;
		if(rootPage == -1) {
			return -1;
		}
	}
		//cout<<"fails in getLeafNode"<<endl;
	//find the parent and the leaf
	free(buffer);
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
		//if this is rootPAGE, change it to a non-root page;
		isRoot = 0;
	}

	short r_keyNum = totalKeys - keyNum - 1;
	short r_offset = offset - start + 8;
	writeDirectory(newPage, isRoot, r_keyNum, isLeaf, nextPage, r_offset);
	fileHandle.appendPage(newPage);
	r_pageNum = fileHandle.getNumberOfPages() - 1;
	start -= keyLen;
	writeDirectory(buffer, isRoot, keyNum, isLeaf, r_pageNum, start);
	//fileHandle.writePage(c_pageNum, buffer);
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

RC insertInternalNode(FileHandle & fileHandle, const Attribute & attribute, short & pageNum, short &leftLeaf, short &rightLeaf, void *key, stack<short> &parentStack) {
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
	short l_pageNum = pageNum;
	fileHandle.readPage(pageNum, buffer);
	short isRoot, totalKeys, isLeaf, nextPage, offset;
	bool doubled = false;
	getDirectory(buffer, isRoot, totalKeys, isLeaf, nextPage, offset, doubled);
	/***********need check *******/
	short parent = -1; // if this page is root;
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
		//THINK HERE if we can have a key which is equal to the original key in internal node?
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
	memcpy((char *)buffer + 2, &totalKeys, 2); //update totalKeys;

	if(split) {
		memcpy((char *)buffer + PAGE_SIZE + PAGE_SIZE - 2, &offset, 2);
		short r_pageNum;
		void *upperKey = malloc(54);
		//void *newPage = malloc(PAGE_SIZE);
		splitInternalNode(buffer, attribute, upperKey, pageNum, r_pageNum, fileHandle);
		fileHandle.writePage(pageNum, buffer);
		free(buffer);
		//insertInternalNode(fileHandle, attribute, parent, pageNum, r_pageNum, upperKey, parentStack);

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
		fileHandle.readPage(overflowPageNum, overflowPage);
		short freeSpaceStartAtOP = *(short *)((char *)overflowPage+PAGE_SIZE - 4);
		if(freeSpaceStartAtOP + 2*2 + 4 <= PAGE_SIZE){//the first overflow page is not full
			//insert to this page
			short value1 = rid.pageNum;
			short value2 = rid.slotNum;
			memcpy((char *)overflowPage + freeSpaceStartAtOP, &value1 , 2);
			memcpy((char *)overflowPage + freeSpaceStartAtOP + 2, &value2, 2);
			//nothing for the leaf page to be changed, but to update the directory of overflow page
			freeSpaceStartAtOP += 4;
			memcpy((char *)overflowPage + PAGE_SIZE - 4, &freeSpaceStartAtOP, 2);
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
				memcpy(newOverflowPage, &rid.pageNum, 2);
				memcpy((char *)newOverflowPage + 2, &rid.slotNum, 2);
				short flags = -1;
				memcpy((char *)newOverflowPage+PAGE_SIZE-2,&flags,2);//next pointer
				flags = 4;
				memcpy((char *)newOverflowPage+PAGE_SIZE-4, &flags, 2);//free space start offset
				RC rc1 = fileHandle.appendPage(newOverflowPage);
				free(buffer);
				free(overflowPage);
				free(newOverflowPage);
				return rc1;

			} else{
				//the current overflow page is not full, we can insert to it
				memcpy((char *)overflowPage+freeSpaceStartAtOP, &rid.pageNum, 2);
				memcpy((char *)overflowPage+freeSpaceStartAtOP + 2, &rid.slotNum, 2);

				freeSpaceStartAtOP += 4;
				memcpy((char *)overflowPage+PAGE_SIZE - 4, &freeSpaceStartAtOP, 2);
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
		memcpy((char *)firstOverflowPage+4, &rid.pageNum, 2);
		memcpy((char *)firstOverflowPage + 6, &rid.slotNum, 2);
		short flag = -1;
		memcpy((char *)firstOverflowPage+PAGE_SIZE-2,&flag,2);
		flag = 8;
		memcpy((char *)firstOverflowPage+PAGE_SIZE-4,&flag,2);

		fileHandle.appendPage(firstOverflowPage);

				//update the slot in leaf
		flag = -1;
		memcpy((char *)buffer+offset+keyLen, &flag, 2);
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
		void *midEntry,
		short &rightChildPageNum,
		bool &updateParent
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
			int len = attribute.type==TypeVarChar ? *(int *)((char *)buffer + offset) + 4 + 4 : 4 + 4;
			offset += len;

		}
	}
	if(isExist){
		//duplicate operation is the same, whether the leaf is full or not
		RC rc5 = insertDuplicate(fileHandle, attribute, buffer, offset,pageNum, key, keyLen, rid);
		return rc5;
	}
	if(freeSpaceOffset + keyLen + 2*2 +2 <= PAGE_SIZE){ //the leaf is not full, insert directly
			//normal insertion
			memmove((char *)buffer + offset + keyLen + 4, (char *)buffer + offset, freeSpaceOffset - offset);
			memcpy((char *)buffer + offset, key, keyLen);
			memcpy((char *)buffer+offset+keyLen, &rid.pageNum, 2);
			memcpy((char *)buffer+offset+keyLen+2, &rid.slotNum, 2);

			totalEntries += 1;
			memcpy((char *)buffer+2, &totalEntries, 2);
			freeSpaceOffset += (keyLen+4);
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
		short rightEntriesNum = totalEntries - (totalEntries/2);


		//set up right page's directory & entries, append it

		/*memcpy(rightPage, &parentNum, 2);
		memcpy((char *)rightPage+2, &rightEntriesNum, 2);
		memcpy((char *)rightPage+4, &isLeaf, 2);
		memcpy((char *)rightPage+6, &rightSibling, 2);*/
		short rightFreeSpaceOffset = freeSpaceOffset - midPoint + 8;
		//memcpy((char *)right+PAGE_SIZE-2,&rightFreeSpaceOffset, 2);
		writeDirectory(rightPage, parentNum, rightEntriesNum, isLeaf,  rightSibling, rightFreeSpaceOffset);

		memmove((char *)rightPage + 8, (char *)tempPage+midPoint,freeSpaceOffset-midPoint);
		RC rc2 = fileHandle.appendPage(rightPage);

		if(rc2<0) {

			return rc2;
		}
		//find the entry to lift

		short midEntryLen  =  attribute.type==TypeVarChar ? *(int *)((char *)rightPage + 8)+4 :4 ;
		//midEntry = malloc(midEntryLen);

		memcpy(midEntry, (char *)rightPage + 8, midEntryLen);
		//update left leaf page
		totalEntries /= 2;
		//memcpy(buffer, &parentNum, 2);
		//memcpy((char *)buffer+2, &totalEntries, 2);
		//memcpy((char *)buffer+4, &isLeaf, 2);
		short newRightSibling = fileHandle.getNumberOfPages()-1;
		//memcpy((char *)buffer+6, &newRightSibling, 2);
		//memcpy((char *)buffer+PAGE_SIZE-2, &midPoint, 2);
		writeDirectory(buffer, parentNum, totalEntries, isLeaf,  newRightSibling, midPoint);
		memcpy((char *)buffer + 8, (char *)tempPage + 8, midPoint-8);
		RC rc3 = fileHandle.writePage(pageNum,buffer);
		//push the mid entry to the parent node, update the index:
		updateParent = true;
		rightChildPageNum = newRightSibling;
		free(buffer);
		free(tempPage);
		free(rightPage);
		//free(midEntry);
		return rc3;
	}
}


RC IndexManager::insertEntry(IXFileHandle &ixfileHandle,
		const Attribute &attribute,
		const void *key,
		const RID &rid)
{
	//FileHandle fileHandle = ixfileHandle.fileHandle;
	if(! ixfileHandle.fileHandle.alreadyOpen()) return -1;

	unsigned int keyLen=0;
	RC rc;
	if(ixfileHandle.fileHandle.getNumberOfPages() ==0){
		//if the number of pages is 0: build a root(which is also a leaf)
		//while building the root, also insert the key
		rc = firstLeaf(ixfileHandle, attribute, key, keyLen, rid);

		//and copy to the page directly
	}else{
		//RC rc2 = insertToLeaf();
		//return rc2;
		//there is no need for parent, just insert at this page and check for split
		stack<short>internalStack;
		rc = getLeafNode(ixfileHandle.fileHandle, attribute, key, internalStack);
		if(rc < 0) return rc;

		/***********need check**********/

		short leafPage = internalStack.empty() ? 1 : internalStack.top();
		if(! internalStack.empty()){
			internalStack.pop();
		}
		short rightChildPageNum = -1;
		bool updateParent = false;
		void *keyToLift = malloc(54);
		rc = insertToLeaf(ixfileHandle.fileHandle,attribute,leafPage, key, rid , keyToLift, rightChildPageNum, updateParent);
		if(updateParent && rightChildPageNum > 0){
			short parentPageNum = internalStack.empty() ? -1 : internalStack.top();
			if(! internalStack.empty()){
				internalStack.pop();
			}
			insertInternalNode(ixfileHandle.fileHandle, attribute, parentPageNum, leafPage, rightChildPageNum, keyToLift, internalStack);

		}
		free(keyToLift);
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
	//cout<<"insert entry success"<<endl;
	return rc;
}

void getEntryFromOFP( FileHandle &fileHandle, void *overflow, const short &pageNum, const RID &rid, short &newOffset, bool &isFound){
	fileHandle.readPage(pageNum, overflow);
	newOffset = 0;
	short freeSpaceOffset = *(short *)((char *)overflow+PAGE_SIZE-4);
	short cursor = 0;
	isFound = false;
	short currentPageNum, currentSlotNum;
	//search in the current page:
	for(; cursor<freeSpaceOffset; cursor +=4){
		currentPageNum = *(short *)((char *)overflow + cursor);
		currentSlotNum = *(short *)((char *)overflow + cursor + 2);
		if(currentPageNum==rid.pageNum && currentSlotNum==rid.slotNum){
			newOffset = cursor;
			isFound = true;
			break;
		}
	}
}

RC deleteEntryInLeaf(FileHandle &fileHandle, const short &leafPageNum, const Attribute &attribute, const void *key, const RID &rid){
	if(!fileHandle.alreadyOpen()) return -1;
	void *buffer = malloc(PAGE_SIZE);
	RC rc = fileHandle.readPage(leafPageNum,buffer);
	if(rc<0) return rc;

	bool doubled = false;
	short isRoot, totalEntries, isLeaf, rightSibling, freeSpaceOffset;
	getDirectory(buffer,isRoot, totalEntries, isLeaf, rightSibling, freeSpaceOffset,doubled);
	//if(rc<0) return rc;

	//compare and find the entry to delete
	bool isExist = false;
	short offset = 8;
	for(int i=0; i<totalEntries; i++){
		int res= compareWithKey(buffer, offset, key, attribute);
		if(res>=0){
			if(res==0) isExist=true;
			break;
		}else {
			int len = attribute.type==TypeVarChar ? *(int *)((char *)buffer + offset)+4+4 : 4 + 4;
			offset += len;
		}
	}
	if(! isExist) return -1; //no entry satisfies, wrong
	//if exist, delete
	int keyLen = attribute.type==TypeVarChar ? *(int *)key + 4 :4 ;
	short entryPageNum = *(short *)((char *)buffer+offset+keyLen);
	short entrySlotNum = *(short *)((char *)buffer+offset+keyLen+2);

	if(entryPageNum != -1){// there is no overflow page, we only need to delete this one
		if(entryPageNum==rid.pageNum && entrySlotNum==rid.slotNum){
			short endOffset = offset+keyLen+4;
			memmove((char *)buffer+offset, (char *)buffer+endOffset, freeSpaceOffset-endOffset);
			totalEntries -= 1;
			memcpy((char *)buffer+2, &totalEntries, 2);
			freeSpaceOffset -= (keyLen+4);
			memcpy((char *)buffer+PAGE_SIZE-2, &freeSpaceOffset, 2);
			rc = fileHandle.writePage(leafPageNum,buffer);
			free(buffer);
			return rc;
		} else{//wrong RID
			free(buffer);
			return -1;
		}
	}else{//there is overflow page, we need to read through and delete the right rid
		void *overflow = malloc(PAGE_SIZE);
		short newOffset=0;
		bool isFound=false;
		short currentPageNum = entrySlotNum;

		getEntryFromOFP(fileHandle,overflow, currentPageNum, rid, newOffset, isFound);
		short nextPageNum = *(short *)((char *)overflow+PAGE_SIZE-2);
		short freeSpaceOffset  = *(short *)((char *)overflow+PAGE_SIZE-4);

		if(!isFound){ //not found in the first page, then search all the following pages
			while(nextPageNum != -1){
				currentPageNum = nextPageNum;
				getEntryFromOFP(fileHandle,overflow, currentPageNum, rid, newOffset, isFound);
				if(isFound) break;
				nextPageNum = *(short *)((char *)overflow +PAGE_SIZE -2);
			}
		}
		if(isFound){
			//delete in the current overflow page
			memmove((char *)overflow+newOffset, (char *)overflow+newOffset+4, freeSpaceOffset-newOffset-4);
			freeSpaceOffset -= 4;
			memcpy((char *)overflow+PAGE_SIZE-4, &freeSpaceOffset, 2);
			rc = fileHandle.writePage(currentPageNum,overflow);
			free(overflow);
			free(buffer);
		}else{
			//not found the rid, wrong
			free(overflow);
			free(buffer);
			rc = -1;
		}
		return rc;
	}
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	//FileHandle fileHandle = ixfileHandle.fileHandle;

	stack<short> internalStack;
	RC rc = getLeafNode(ixfileHandle.fileHandle, attribute, key, internalStack);
	if(rc<0){
		return rc;
	}
	short leafPage = internalStack.empty() ? 1 : internalStack.top();
	if(!internalStack.empty()){
		internalStack.pop();
	}

	RC rc1 = deleteEntryInLeaf(ixfileHandle.fileHandle, leafPage, attribute, key, rid);

    return rc1;
}


string printOverflowPage(FileHandle &fileHandle, const short &pageNum){
	string output = "";
	void *overflow = malloc(PAGE_SIZE);
	fileHandle.readPage(pageNum,overflow);
	short freeSpaceOffset = *(short *)((char *)overflow+PAGE_SIZE-4);
	short nextPageNum = *(short *)((char *)overflow + PAGE_SIZE -2);
	short start = 0;
	short ridPageNum, ridSlotNum;
	//first page
	for(int i=0;i<freeSpaceOffset/4; i++){
		ridPageNum = *(short *)((char *)overflow+start);
		start += 2;
		ridSlotNum = *(short *)((char *)overflow+start);
		start += 2;

		output = output + "(" + to_string(ridPageNum) + "," + to_string(ridSlotNum) +")";
		if(i<freeSpaceOffset/4-1) output += ",";
	}
	//(1,1),(1,2),(2,3)
	//following overflow pages
	while(nextPageNum != -1){
		fileHandle.readPage(nextPageNum, overflow);
		nextPageNum = *(short *)((char *)overflow + PAGE_SIZE -2);
		freeSpaceOffset = *(short *)((char *)overflow+PAGE_SIZE-4);
		start = 0;
		for(int i=0;i<freeSpaceOffset/4; i++){
			ridPageNum = *(short *)((char *)overflow+start);
			start += 2;
			ridSlotNum = *(short *)((char *)overflow+start);
			start += 2;

			output = output + ",(" + to_string(ridPageNum) + "," + to_string(ridSlotNum) +")";
		}
	}
	return output;
}
//printLeaf(fileHandle,node,attribute,depth,totalEntries);
void printLeaf(FileHandle &fileHandle, void *leafNode, const Attribute &attribute, const int &depth, const short &totalEntries){
	if(totalEntries == 0) return;

	for (int i=0; i<depth; i++){
		printf("\t");
	}
	printf("{\"Keys\":[");
	//bool isExist = false;
	short start = 8;
	for (int i=0; i<totalEntries; i++){
		if(i!= 0) printf(",");
		printf("\"");
		switch(attribute.type){
		case TypeInt:
			printf("%d:", *(int *)((char *)leafNode+start));
			start += 4;
			break;
		case TypeReal:
			printf("%.1f:",*(float *)((char *)leafNode+start));
			start += 4;
			break;
		case TypeVarChar:
			int stringLen = *(int *)((char *)leafNode + start);
			for(int j=0; j<stringLen; j++){
				printf("%c ",*((char *)leafNode+start+4+j));
			}
			printf(":");
			start += (4+stringLen);
			break;
		}
		printf("[");
		//check if there is overflow page
		short entryPageNum = *(short *)((char *)leafNode+start);
		short entrySlotNum = *(short *)((char *)leafNode+start+2);
		start += 4;
		if(entryPageNum == -1){
			//isExist = true;
			//overflow page
			string output = printOverflowPage(fileHandle, entrySlotNum);
			cout<<output;
		}else{// only one record
			printf("(%d,", entryPageNum);
			printf("%d)", entrySlotNum);
		}
		printf("]\"");
	}
	printf("]}\n");
}
//printInternalNode(fileHandle, attribute, 0, rootPageNum);
void printInternalNode(FileHandle &fileHandle, const Attribute &attribute, const int &depth, const short &printPageNum){
	void *node = malloc(PAGE_SIZE);
	RC rc = fileHandle.readPage(printPageNum,node);
	if(rc<0) return;

	short isRoot, totalEntries, isLeaf, rightSibling, freeSpaceOffset;
	bool doubled = false;
	getDirectory(node,isRoot, totalEntries, isLeaf, rightSibling, freeSpaceOffset,doubled);

	if(isLeaf == 1){
		printLeaf(fileHandle,node,attribute,depth,totalEntries);
		free(node);
		return;
	}
	//print internal nodes
	for(int i=0; i<depth ; i++){
		printf("\t");
	}
	printf( "{\"Keys\":[");

	vector<short> childrenVector;
	//short end = *(short *)((char *)node+PAGE_SIZE-2);
	short start = 8;
	short childNum = *(short *)((char *)node + start);
	childrenVector.push_back(childNum);

	start += 2;
	for (short i=0; i<totalEntries; i++){
		if(i != 0) printf(",");
		switch(attribute.type){
		case TypeInt:
			printf("%d ", *(int *)((char *)node+start));
			start += 4;
			break;
		case TypeReal:
			printf("%.1f ",*(float *)((char *)node +start));
			start += 4;
			break;
		case TypeVarChar:
			int stringLen = *(int *)((char *)node + start);
			printf("\"");
			for(int j=0; j<stringLen; j++){
				printf("%c ",*((char *)node+start+4+j));
			}
			printf("\"");
			start += 4+stringLen;
			break;
		}
		childNum = *(short *)((char *)node + start);
		childrenVector.push_back(childNum);
		start += 2;
	}
	printf("],\n");
	for(int i=0; i<depth ; i++){
		printf("\t");
	}
	printf( "{\"Children\":[\n");
	//print out children
	int numOfChildren = childrenVector.size();
	for(int i=0; i<numOfChildren; i++){
		printInternalNode(fileHandle, attribute, depth+1, childrenVector[i]);
		if(i != numOfChildren-1){
			printf(",");
		}
		printf("\n");
	}

	for(int i=0; i<depth ; i++){
		printf("\t");
	}
	printf( "]}\n");
	free(node);

}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
	//FileHandle fileHandle = ixfileHandle.fileHandle;
	unsigned int numOfPages = ixfileHandle.fileHandle.getNumberOfPages();
	if(numOfPages == 0) return;
	short rootPageNum = getRootPage(ixfileHandle.fileHandle);
	//When root is leaf; this situation will be solved in printInternalNode

	/*if(numOfPages == 2){
		void *root = malloc(PAGE_SIZE);
		fileHandle.readPage(rootPageNum,root);
		//printLeaf(root,attribute,0);
		free(root);
		return;
	}
	*/
	printInternalNode(ixfileHandle.fileHandle, attribute, 0, rootPageNum);
	return;

}



bool isValid(const Attribute &attribute, const void *lowKey, const void *highKey, const bool lowI, const bool highI) {
	if(lowKey == NULL || highKey == NULL) return true;
	short offset = 0;
	 int res = compareWithKey(highKey, offset, lowKey, attribute);
	 if(res < 0) return false;
	 if(res == 0 && (lowI == false || highI == false)){
		 return false;
	 }
	 return true;
}

RC findLeftPage(FileHandle &fileHandle, short &pageNum){
	short rootPage = getRootPage(fileHandle);
	void *buffer = malloc(PAGE_SIZE);
	short isLeaf = 0;

	while(true){
		fileHandle.readPage(rootPage, buffer);
		isLeaf = *(short *)((char *)buffer + 4);
		//cout<<"the root page number is "<<rootPage<<", the is Leaf flag is"<<isLeaf<<endl;
		if(isLeaf == 1){
			//cout<<"true for 980"<<endl;
			break;
		}
		rootPage = *(short *)((char *)buffer + 8);
	}
	//cout<<"go through 998"<<endl;
	pageNum = rootPage;
	free(buffer);
	return 0;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
	if(! ixfileHandle.fileHandle.alreadyOpen() || ixfileHandle.fileHandle.getNumberOfPages() == 0){
		 return -1;
	}
	if(! isValid(attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive)){
		return -1;
	}
	short curPage = -1;
	if(lowKey == NULL){
		findLeftPage(ixfileHandle.fileHandle, curPage);
	}else{
		stack<short> internalStack;
		RC rc1 = getLeafNode(ixfileHandle.fileHandle, attribute, lowKey , internalStack); // find leaf;
		if(rc1 < 0 || internalStack.empty()){
			return -1;
		}
		curPage = internalStack.top();
		internalStack.pop();
	}
	void *page = malloc(PAGE_SIZE);
	ixfileHandle.fileHandle.readPage(curPage, page);
	short offset = lowKey == NULL ? 8 : -1;
	//void *key = malloc(54);
	//cout<<"1019"<<endl;
	void *overFlow = malloc(PAGE_SIZE);
	RID rid;
	rid.pageNum = -1;
	rid.slotNum = -1;
//	ix_ScanIterator.initializeSI(ixfileHandle, ixfileHandle.fileHandle, attribute, curPage,
//		        		offset, lowKeyInclusive, highKeyInclusive, key, page, lowKey, highKey, overFlow, rid);
	ix_ScanIterator.initializeSI(ixfileHandle, ixfileHandle.fileHandle, attribute, curPage, offset, lowKeyInclusive, highKeyInclusive, page, lowKey, highKey, overFlow, rid);
	return 0;

}



IX_ScanIterator::IX_ScanIterator()
{
	curPage = -1;
	lowIncluded = false;
	highIncluded = false;
	offSet = -1;
	curLeaf = NULL;
	low = NULL;
	high = NULL;
	overFlow = NULL;
	overFlowOffset = -1;
//	inOverFlow = false;

}

IX_ScanIterator::~IX_ScanIterator()
{
}
bool isEmpty(void *buffer, const short &offset){
	int temp = *(int *)((char *)buffer + offset);
	if(temp == NULL) return true;
	return false;
}

bool validOffset(const void* page, const short &offset, const void *low, const void *high, const Attribute &attribute, const bool lowI, const bool highI){
	if(low==NULL && high==NULL) return true;
	int comp1;
	if(low==NULL){
		comp1 = 1;
	}else{
		comp1 = compareWithKey(page, offset, low, attribute);
	}
	int comp2;
	if(high == NULL){
		comp2 = -1;
	}else{
		comp2 = compareWithKey(page, offset, high, attribute);
	}

	if((comp1 == 0 && lowI) || comp1 > 0){
		if((comp2 == 0 && highI) || comp2 < 0){
				return true;
		}
	}
	return false;
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	if(offSet == -2) return IX_EOF;
	bool empty;
	short parent, totalKeys, leaf, nextPage, freeSpace;
	bool doubled = false;
	getDirectory(curLeaf, parent, totalKeys, leaf, nextPage, freeSpace, doubled);
	while(offSet == -1 || offSet >= freeSpace){
		if(low == NULL) offSet = 8;
		else{
			//this is for the first time scan to find the starting point.

			short start = 8;
			for(int i = 0; i < totalKeys; i++) {
				int res = compareWithKey(curLeaf, start, low, attribute);
				if(res >= 0) {
					offSet = start;
					break;
				}
				start += attribute.type == TypeVarChar? *(int *)((char *)curLeaf + start) + 4 : 4;
				start += 4;
			}
			empty = isEmpty(curLeaf, offSet);
			if(offSet == 8 && empty && nextPage==-1){
				return IX_EOF;
			}
			//if(offSet == 8 && nextPage == -1) return IX_EOF;
			fileHandle.readPage(nextPage, curLeaf);
			curPage = nextPage;
			getDirectory(curLeaf, parent, totalKeys, leaf, nextPage, freeSpace, doubled);
		}
	}
//	if(offSet == -1){
//		if(low == NULL) offSet = 8;
//		else{
//			//this is for the first time scan to find the starting point.
//			short start = 8;
//			for(int i = 0; i < totalKeys; i++) {
//				int res = compareWithKey(curLeaf, start, low, attribute);
//				if(res >= 0) {
//					offSet = start;
//					break;
//				}
//				start += attribute.type == TypeVarChar? *(int *)((char *)curLeaf + start) + 4 : 4;
//				start += 4;
//			}
//
//		}

	//offSet == -1 may happen when this page has been totally deleted, then need to move on to nextpage;
//	if(offSet == -1 || offSet >= freeSpace){
//		if(nextPage == -1) return IX_EOF;
//		fileHandle.readPage(nextPage, curLeaf);
//		offSet = -1;
//		curPage = nextPage;
//	}else
//	{
	//here is just for overflow page detection:
		if(validOffset(curLeaf,offSet, low, high, attribute, lowIncluded, highIncluded)){
			int keyLen = attribute.type == TypeVarChar? *(int *)((char *)curLeaf + offSet) + 4 : 4;
			memcpy(key, (char *)curLeaf + offSet, keyLen);
			while(*(short *)((char *)curLeaf + offSet + keyLen) == -1){

				if(overFlowOffset == -1){
					short overflowPage = *(short *)((char *)curLeaf + offSet + keyLen + 2);////!!!!!!!!!!!!!!!!
					fileHandle.readPage(overflowPage, overFlow);
					overFlowOffset = 0;
					cout<<"go through 1156"<<endl;
				}
				short freeSpaceOF = *(short *)((char *)overFlow + PAGE_SIZE - 4);
				short nextOF = *(short *)((char *)overFlow + PAGE_SIZE-2);

				while(overFlowOffset >= freeSpaceOF && nextOF != -1){
					fileHandle.readPage(nextOF, overFlow);
					freeSpaceOF = *(short *)((char *)overFlow + PAGE_SIZE - 4);
					nextOF = *(short *)((char *)overFlow + PAGE_SIZE-2);
					overFlowOffset = 0;
				}
				empty = isEmpty(overFlow, overFlowOffset);
				if(empty && nextOF == -1){

					offSet += (keyLen + 4);
					if(! validOffset(curLeaf,offSet, low, high, attribute, lowIncluded, highIncluded)){
						return IX_EOF;
					}
					keyLen = attribute.type == TypeVarChar? *(int *)((char *)curLeaf + offSet) + 4 : 4;
					memcpy(key, (char *)curLeaf + offSet, keyLen);
					overFlowOffset = -1;

				}
				else{

					rid.pageNum = *(short *)((char *)overFlow + overFlowOffset);
					rid.slotNum = *(short *)((char *)overFlow + overFlowOffset + 2);
				    overFlowOffset += 4;
				    return 0;

				}
			}
			rid.pageNum = *(short *)((char *)curLeaf + offSet + keyLen);
			rid.slotNum = *(short *)((char *)curLeaf + offSet + keyLen +2);
			offSet += (keyLen + 4);

		}else{
			return IX_EOF;
		}


	if(offSet >= freeSpace) {
		if(nextPage == -1) {
			offSet = -2;
		}else{
			fileHandle.readPage(nextPage, curLeaf);
			offSet = 8; //that is for the non-first page scan;
		   curPage = nextPage;
		}

	}

	return 0;
}


//	int comp1 = compareWithKey(curLeaf, offSet, low, attribute);
//	int comp2 = compareWithKey(curLeaf, offSet, high, attribute);
//	if((comp1 == 0 && lowIncluded) || comp1 > 0){
//		if((comp2 == 0 && highIncluded) || comp2 < 0){
//			int keyLen = attribute.type == TypeVarChar? *(int *)((char *)curLeaf + offSet) + 4 : 4;
//			memcpy(key, (char *)curLeaf + offSet, keyLen);
//			if(*(short *)((char *)curLeaf + offSet + keyLen) == -1){
//				if(overFlowOffset == -1){
//					short overflowPage = *(short *)((char *)curLeaf + offSet + keyLen) + 2;
//					fileHandle.readPage(overflowPage, overFlow);
//					overFlowOffset = 8;
//					//inOverFlow = true;
//				}
//				short freeSpaceOF = -1;
//				short nextOF = 0;
//				while(nextOF != -1){
//					freeSpaceOF = *(short *)((char *)overFlow + PAGE_SIZE - 4);
//					nextOF = *(short *)((char *)overFlow + PAGE_SIZE-2);
//					if(overFlowOffset < freeSpaceOF){
//						rid.pageNum = *(short *)((char *)overFlow + overFlowOffset);
//						rid.slotNum = *(short *)((char *)overFlow + overFlowOffset + 2);
//						overFlowOffset += 4;
//						if(overFlowOffset == freeSpaceOF && nextOF == -1){
//							offSet += (keyLen + 4);
//							break;
//						}else if(overFlowOffset == freeSpaceOF){
//							fileHandle.readPage(nextOF, overFlow);
//							overFlowOffset = 8;
//						}
//
//					}else{
//						if(nextOF == -1){
//							offSet += (keyLen + 4);
//
//						}
//					}
//
//
//				}
//				if(overFlowOffset >= freeSpaceOF){
//					if(nextOF == -1){
//						offSet += (keyLen + 4);
//					}
//					else{
//						fileHandle.readPage(nextOF, overFlow);
//						overFlowOffset = 8;
//					}
//				}
//				rid.pageNum = *(short *)((char *)overFlow + overFlowOffset);
//				rid.slotNum = *(short *)((char *)overFlow + overFlowOffset + 2);
//				overFlowOffset += 4;
//
//			}else{
//				rid.pageNum = *(short *)((char *)curLeaf + offSet + keyLen);
//				rid.slotNum = *(short *)((char *)curLeaf + offSet + keyLen) + 2;
//				offSet += (keyLen + 4);
//			}
//		}
//	}



RC IX_ScanIterator::close()
{
    low = NULL;
    high = NULL;
	free(overFlow);
    free(curLeaf);

	return 0;
}


IXFileHandle::IXFileHandle()
{
	//fileHandle = FileHandle();
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
	ixReadPageCounter = fileHandle.readPageCounter;
    ixWritePageCounter = fileHandle.writePageCounter;
    ixAppendPageCounter = fileHandle.appendPageCounter;

    readPageCount = ixReadPageCounter;
    writePageCount = ixWritePageCounter;
    appendPageCount = ixAppendPageCounter;

	return 0;
}

