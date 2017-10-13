#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    PagedFileManager *pfm = PagedFileManager::instance();
    return pfm->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	PagedFileManager *pfm = PagedFileManager::instance();
	return pfm->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
	PagedFileManager *pfm = PagedFileManager::instance();
	return pfm->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
	PagedFileManager *pfm = PagedFileManager::instance();
	return pfm->closeFile(fileHandle);
}

//Record Format:
//  Number of Attribute| Attribute 1 offset | Attribute 2 offset | Attribute 3 offset | Attribute 1 content | Attribute 2 content | Attribute 3 content|
//Directory Format:
//  slot N offset | slot N-1 offset | ......|slot 1 offset| F (free space) | N ( total number of RID)
// offset stands for ending position.
// if a record has been deleted, the slot offset in the directory will be set to -1;
// after a deletion, inserting a new record will not occupy the old RID, every record has a unique RID.

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    if(! fileHandle.alreadyOpen()){
    		return -1;
    }

	void *header = malloc(1600); // header size is no larger than 1500 bytes.
	void *page = malloc(PAGE_SIZE);
	int fieldSize = recordDescriptor.size();
	int pointerSize = ceil((double) recordDescriptor.size() / 8);

	int size = formatHeader(data, header, recordDescriptor, fieldSize, pointerSize); //total size of this record
	if(size <= 0) return -1;

    short start = locatePos(size, rid, fileHandle);          // find a position for this record
    short end = start + size; 					           //record ending position
    short freesize = PAGE_SIZE - (rid.slotNum + 2)*2 - end; //update free space of this page


    fileHandle.readPage(rid.pageNum -1, page);
    memcpy((char *)page + PAGE_SIZE - 2, (void *)&rid.slotNum, 2); // update total number of records;
    memcpy((char *)page + PAGE_SIZE - 4, (void *)&freesize, 2);    // update free space available on this page;
    memcpy((char *)page + start, header, 2*fieldSize + 2);
    memcpy((char *)page + start + 2*(1+fieldSize), (char *)data + pointerSize, size -(fieldSize + 1)*2); // update content of the record;
    memcpy((char *)page + PAGE_SIZE - (rid.slotNum + 2)*2, (void *)&end, 2); //update where to begin for next record;

    fileHandle.writePage(rid.pageNum - 1, page);

    free(page);
    free(header);
    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	int SHIFT = 8;
	unsigned int pageNum = rid.pageNum -1;
	unsigned int slotNum = rid.slotNum;
	// check if fileHandle is closed or pageNum if out of range:
	if(! fileHandle.alreadyOpen() || fileHandle.getNumberOfPages() -1 < pageNum){
		return -1;
	}
	void *page = malloc(PAGE_SIZE);
	fileHandle.readPage(pageNum, page);
	//check if slot number larger than the total slot N;
	short N = *(short *)((char *)page + PAGE_SIZE - 2);
	if(slotNum > (unsigned) N){
		free(page);
		return -1;
	}
	// check if record exist or has been deleted:
	short offSet = *(short *)((char *)page + PAGE_SIZE - (slotNum + 2)*2);
	if(offSet == -1){
		free(page);
		return -1;
	}
	// find record start position in the page:
	short startPos = 0;
	for(short i = slotNum; i >= 1; i--) {
		if(i == 1){
			startPos = 0;
		}else{
			startPos = *(short *)((char *)page + PAGE_SIZE - (i+1)*2);
			if(startPos != -1) break;
		}
	}
	// check if record has been relocated:
	if(*(short *)((char *)page + startPos) == -1){
		short newPageNum = *(short *)((char *)page + startPos + 2);
		short newSlotNum = *(short *)((char *)page + startPos + 4);
		RID  newRid;
		newRid.pageNum = newPageNum;
		newRid.slotNum = newSlotNum;
		free(page);
		return readRecord(fileHandle, recordDescriptor,newRid,data);
	}else{
		short startPos = 0;
		startPos = slotNum == 1? 0 : *(short *)((char *)page + PAGE_SIZE - (slotNum + 1)*2);
		int pointerSize = ceil((double) recordDescriptor.size() / SHIFT);
		int fieldSize = recordDescriptor.size();
		short recordLen = *(short *)((char *)page + startPos + 2*fieldSize);

		int nullArray[fieldSize];
		short offset = *(short *)((char *)page + startPos + 2);
		nullArray[0] = offset == 0? 1 : 0;

		for(int i = 1; i < fieldSize; i++) {
			short nextOffset = *(short *)((char *)page + startPos + 2*(i+1));
			nullArray[i] = offset == nextOffset ? 1 : 0;
			offset = nextOffset;
		}
        
        unsigned char *nullPointer = (unsigned char *) malloc(pointerSize);

        for(int i = 0; i < pointerSize; i++) {
            int sum = 0;
            int j = SHIFT*i;
            while(j < (i+1)*SHIFT && j < fieldSize){
                sum |= nullArray[j]*(1 << (SHIFT -1 - j%SHIFT));
                j++;
            }
            *(nullPointer + i) = sum;
        }

		memcpy((char *)data, nullPointer, pointerSize);
		memcpy((char *) data + pointerSize, (char *)page + startPos + 2*(fieldSize + 1), recordLen);
		free(nullPointer);
		free(page);
	}


	return 0;
}



RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
	int SHIFT = 8;
	int fieldSize = recordDescriptor.size();    
	int start = (int) ceil((double) recordDescriptor.size() / SHIFT);
    	for(int i = 0; i < fieldSize; i++) {
    		bool null = *((unsigned char *)data + i/SHIFT) & (1 << (SHIFT - 1 - i%SHIFT));
    		Attribute attribute = recordDescriptor[i];
    		if(null){
    			cout << attribute.name <<": " << "NULL";
    		} else {
    			switch(attribute.type) {
    			case TypeInt:
    				cout << attribute.name <<": " << *(int *)((char *)data + start) ;
				start += 4;
				break;
    			case TypeReal:
    				cout << attribute.name << ": " << *(float *)((char *)data + start);
    				start += 4;
    				break;
    			case TypeVarChar:
    				int len = *(int *)((char *)data + start);
    				start += 4;
    				cout << attribute.name << ": ";
				for(int j = 0; j < len; j++) {
					cout << *((char *)data + start + j);
				}
    				start += len;
    				break;
    			}
    		}
    }
    cout << endl;
    return 0;
}

// Assisting Methods listed below:

short locatePos(int size, RID &rid, FileHandle &fileHandle){
	int curPage = fileHandle.getNumberOfPages();
	int totalPage = fileHandle.getNumberOfPages();
	unsigned short slot = 1; // initial value for slot number for this new record;
	void *data = malloc(PAGE_SIZE);
	short start = 0;
	if(curPage == 0){ //if entire file is empty
		fileHandle.appendPage(data);
		curPage++;
		slot = 1;
		start = 0;
	}else{
		fileHandle.readPage(curPage - 1, data);
		short N = *(short *)((char *)data + PAGE_SIZE - 2);
		start = *(int *)((char *)data + PAGE_SIZE - 2*(N + 2));
		short F = *(short *)((char *)data + PAGE_SIZE - 2*2);
		if(F < size + 2){ // need to find page before.
			for(int i = 1; i < curPage; i++){
				fileHandle.readPage(i-1, data);
				N = *(short *)((char *)data + PAGE_SIZE - 2);
				start = *(int *)((char *)data + PAGE_SIZE - 2 * (N + 2));
				F = *(short *)((char *)data + PAGE_SIZE - 2*2);
				if(N == 0 || F >= size + 2){
					curPage = i;
					slot = N+1;
					break;
				}
			}
			if(curPage == totalPage){ //not find any page before current page.
				int success = fileHandle.appendPage(data);
				if(success < 0) return success;
				curPage++;
				slot = 1;
				start = 0;
			}

		}else{ //find space on last page.
			slot = N + 1;
		}

	}
	free(data);
	rid.pageNum = curPage;
	rid.slotNum = slot;
	return start;
}


int formatHeader(const void *record, void *header, const vector<Attribute> &recordDescriptor, const int &fieldSize, const int &pointerSize) {
	int SHIFT = 8;
	short *pointer = new short[fieldSize + 1];
	int start = (fieldSize + 1) * 2;
	pointer[0] = fieldSize;

	for(int i = 1; i <= fieldSize; i++) {
		bool null = *((unsigned char *)record + (i - 1)/SHIFT) & (1 << (SHIFT - 1 - (i-1)%SHIFT));
		Attribute attribute = recordDescriptor[i-1];
		if(null){
			if(i == 1) {
				pointer[i] = 0;
			}
			else {
				pointer[i] = pointer[i-1];
			}
		}else{
			switch(attribute.type){
			case TypeInt:
			case TypeReal:
				pointer[i] = pointer[i-1] + 4;
				start += 4;
				break;
			case TypeVarChar:
				int len = *(int *)((unsigned char *)record + pointerSize + start - (fieldSize + 1)* 2) + 4;
				pointer[i] = pointer[i-1] + len;
				start += len;
				break;
			}
		}
	}
	for(int i = 0; i <= fieldSize; i++) {
		memcpy((char *)header + 2*i, &pointer[i], 2);
	}
	return start;
}

RC directoryUpdate(void *page, const short N,RID rid,short start, short end, short newOffset){
	if(rid.slotNum > (unsigned)N) return -1;

	memset((char *)page + PAGE_SIZE-2*(rid.slotNum+1),newOffset, 2);
	for (short i = rid.slotNum+1; i <= N; i++) {
			short oldOffset = *(short *)((char *)page + PAGE_SIZE - 2*(i+1));
			if (oldOffset == -1) continue;
			short newOffset = oldOffset-(end-start);
			memset((char *)page + PAGE_SIZE - 2*(i+1), newOffset, 2);
		}
	return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid){
	unsigned int pageNum = rid.pageNum;
	unsigned int slotNum = rid.slotNum;
	if(! fileHandle.alreadyOpen() || fileHandle.getNumberOfPages() < pageNum-1){
		return -1;
	}
	void *page = malloc(PAGE_SIZE);
	fileHandle.readPage(pageNum-1,page);
	short N = *(short *)((char *)page + PAGE_SIZE - 2);
	short freeSpaceOffset = *(short *)((char *)page + PAGE_SIZE-4);
	short end = *(short *)((char *)page+PAGE_SIZE-2*(slotNum+1));
	if(slotNum > (unsigned) N | end==-1){
			free(page);
			return -1;
		}
	short start = *(short *)((char *)page + PAGE_SIZE - 2*(N+1));
	if(start == -1){
		for(int i=N; i>0; i--){
			if(i==1) {
				start = 0;
			} else{
				start = *(short *)((char *)page + PAGE_SIZE - (i+1)*2);
				if(start != -1) break;
			}
		}
	}
	bool isRedirected = *(short *)((char *)page + start);
	if(isRedirected){
		short newPageNum = *(short *)((char *)page + start + 2);
		short newSlotNum = *(short *)((char *)page + start + 4);
		RID  newRid;
		newRid.pageNum = newPageNum;
		newRid.slotNum = newSlotNum;
		free(page);
		return deleteRecord(fileHandle, recordDescriptor,newRid);
	}
	//delete and compact the hole
	memmove((char *)page + start,(char *)page + end, freeSpaceOffset-end);
	//update the directory
	memset((char *)page + PAGE_SIZE-2*(slotNum+1), -1, 2);
	for (short i = slotNum+1; i <= N-1; i++) {
			short oldOffset = *(short *)((char *)page + PAGE_SIZE - sizeof(short)*(i+1));
			if (oldOffset == -1) continue;
			short newOffset = oldOffset-(end-start);
			memset((char *)page + PAGE_SIZE - 2*(i+1), newOffset, sizeof(short));
		}

	int rc = fileHandle.writePage(pageNum-1,page);
	free(page);
	return rc;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid){
	unsigned int pageNum = rid.pageNum;
	unsigned int slotNum = rid.slotNum;
	if(fileHandle.alreadyOpen()){
		return -1;
	}
	void *page = malloc(PAGE_SIZE);
	fileHandle.readPage(pageNum-1,page);

	short N = *(short *)((char *)page + PAGE_SIZE-2);
	short freeSpaceOffset = *(short *)((char *)page + PAGE_SIZE-4);
	short end = *(short *)((char *)page+PAGE_SIZE-2*(slotNum+1));
	if(slotNum > N | end == -1){
			return -1;
		}
	short start = *(short *)((char *)page + PAGE_SIZE - 2*(N+1));
	if(start == -1){
		for(int i=N; i>0; i--){
			if(i==1) {
				start = 0;
			} else{
				start = *(short *)((char *)page + PAGE_SIZE - (i+1)*2);
				if(start != -1) break;
			}
		}
	}
	//int oldDataLen = end - start;
	void *header = malloc(1600);
	int newFieldSize = recordDescriptor.size();
	int newPointerSize = ceil((double) recordDescriptor.size() / 8);
	int newDataLen = formatHeader(data, header, recordDescriptor, newFieldSize, newPointerSize);

	//the old record is redirected
	bool isRedirected = *(short *)((char *)page + start);
	if(isRedirected){
		short newPageNum = *(short *)((char *)page + start + 2);
		short newSlotNum = *(short *)((char *)page + start + 4);
		RID  newRid;
		newRid.pageNum = newPageNum;
		newRid.slotNum = newSlotNum;
		free(page);
		return updateRecord(fileHandle, recordDescriptor,data,newRid);
	}
	//within a page
	if(freeSpaceOffset + newDataLen - (end -start) + 2*(N+1) <= PAGE_SIZE){
		memmove((char *)page + start + newDataLen, (char *)page + end, freeSpaceOffset -end);
		memcpy((char *)page + start,header, 2*(newFieldSize+1));
		memcpy((char *)page + start + 2*(1+newFieldSize), (char *)data + newPointerSize, newDataLen -(newFieldSize + 1)*2);
		//update the directory
		directoryUpdate( page, N, rid, start, end, start+newDataLen);
		fileHandle.writePage(pageNum-1,page);
		free(page);
		return 0;
	}
	//if it can not be added with the current page
	RID newRid;
	int res = insertRecord(fileHandle, recordDescriptor, data, newRid);
	if(res != 0){
		return res;
	}
	memset((char *)page + start, -1, 2);
	memset((char *)page + start+2,newRid.pageNum, 2);
	memset((char *)page + start + 4, newRid.slotNum, 2);
	memmove((char *)page + start + 6, (char *)page + end,freeSpaceOffset-end);

	//update the directory
	directoryUpdate(page, N, rid, start+6, end, start+6);

	fileHandle.writePage(pageNum-1, page);
	free(page);
	return 0;
}

