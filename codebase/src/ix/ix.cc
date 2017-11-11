
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
RC buildRoot(IXFileHandle &ixfileHandle, const Attribute &attribute,const void *key, unsigned int &keyLen, const RID &rid){
	void *root = malloc(PAGE_SIZE);
	memset(root, -1, 4);//set ParentNum to -1
	memset((char *)root+4, 1, 2);//total keys == -1(short)
	memset((char *)root+6, 1, 2); //is leaf
	memset((char *)root+8, -1, 4);//next is empty

	//set the first key entry in the root
	keyLen = attribute.type==TypeVarChar ? *((int *) key) : 4;
	memcpy((char *)root+12, key, keyLen);
	memcpy((char *)root+12+keyLen, &rid.pageNum, 4);
	memcpy((char *)root+16+keyLen, &rid.slotNum, 4);

	short freeSpaceOffset = 20+keyLen;
	memcpy((char *)root+PAGE_SIZE-2, &freeSpaceOffset, 2);
	RC rc = ixfileHandle.fileHandle.appendPageCounter(root);

	free(root);
	return rc;
}
void getDirectory(void *page, int &parentPageNum, short &totalKeys, short &isLeaf, int &rightSibling, short &freeSpaceOffset){
	parentPageNum = *(int *)page;
	totalKeys = *(short *)((char *)page+4);
	isLeaf = *(short *)((char *)page+6);
	rightSibling = *(int *)((char *)page+8);
	freeSpaceOffset = *(short *)((char *)page +PAGE_SIZE-2);
}

int compareWithKey(void *currentPage, short &offset, const void *key, const Attribute &attribute, bool &isExist){
	// -1:key is larger
	// 0: key is equal
	// 1 :key is smaller, this is the right place
	switch(attribute.type){
	case TypeInt:
		int keyValue = *(int *)key;
		int currentValue = *(int *)((char *)currentPage + offset);
		if(keyValue < currentValue) return 1;
		else if(keyValue == currentValue ){
				isExist = true;
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
				isExist = true;
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
			isExist=true;
		}
		return strcmp(currentValue2.c_str(),keyValue2.c_str());
	}
	return -1;
}
//check this again, can this be used in index-index-index-leaf all?
RC getKeyOffsetInParent(void *parentNode, short &totalKeys, short &offset, bool &isExist, const void *key,const Attribute &attribute ){
	//return the offset, which is the position of the child, in which we want to insert key
	int res = -1;
	int keyLen = attribute.type==TypeVarChar ? *((int *) key) : 4;
	int i=0;
	for(; i< totalKeys; i++){
		res = compareWithKey(parentNode, offset, key, attribute, isExist);
		if(res>=0){
			break;
		}else{
			offset += 4+keyLen;
			continue;
		}
	}
	if(i==totalKeys){
		//we searched through all the entries
		short freeSpaceStart = *(short *)((char *)parentNode+PAGE_SIZE-2);
		if(freeSpaceStart != offset) return -1;
	}
	offset -= 4;//so now the offset is for the page number of child
	return 0;
}
RC getKeyOffsetinLeaf(){

	return 0;
}
RC getParent(){
	//find the parent and the leaf
	return 0;
}
//one thing is missing :parent stack!
RC insertToLeaf(IXFileHandle &ixfileHandle,
		const Attribute &attribute,
		int parentPageNum,
		const void* key,
		const RID &rid,
		stack<PageNum> &parentStack){
	/*void *parentNode = malloc(PAGE_SIZE);
	RC rc = ixfileHandle.fileHandle.readPage(parentPageNum,parentNode);
	if(rc<0) return rc;

	//read directory of parent node, find the key offset in parent node
	int parentOfParent, nextIndexNode;
	short totalKeys, isLeaf, freeSpaceOffset;
	getDirectory(parentNode, parentOfParent, totalKeys,isLeaf, nextIndexNode, freeSpaceOffset);

	//get the pageNum of the leaf
	short offsetInParent = 4+2+2+4+4;
	bool isExist = false;
	getKeyOffsetInParent(parentNode, totalKeys, offsetInParent, isExist, key, attribute);
	int leafPageNum = *(int *)((char *)parentNode + offsetInParent);

	void *leaf = malloc(PAGE_SIZE);
	RC rc1 = ixfileHandle.fileHandle.readPage(leafPageNum,leaf);
*/
	return 0;
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
	if(!ixfileHandle.fileHandle.alreadyOpen()) return -1;


	int pageNum = ixfileHandle.fileHandle.getNumberOfPages();
	unsigned int keyLen=0;
	if(pageNum==0){
		//if the number of pages is 0: build a root(which is also a leaf)
		//while building the root, also insert the key
		RC rc1 = buildRoot(ixfileHandle, attribute, key, keyLen, rid);
		return rc1;
		//and copy to the page directly
	}else if(pageNum == 1){
		//RC rc2 = insertToLeaf();
		//return rc2;
		//there is no need for parent, just insert at this page and check for split

	} else{
		getParent();
		//insertToLeaf();
		//else:find the parent and the leaf node for insertion
	}
	//insert to leaf node->
		//if the insertion doesn't need split:just insert
		//else:split the leaf node and push the middle entry into its parent
			//if the parent doesn't need to split: just insert into the node
			//else:split the index node and push the middle entry into its parent, recurse on inserting to parent index node

    return -1;
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

