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


/*RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {


    short freeOffset = start + size;
    short N = *(short *)((char *)page + PAGE_SIZE - 2);
    if(rid.slotNum > (unsigned) N){
    			N = rid.slotNum;
    			memcpy((char *)page + PAGE_SIZE - 2, &N, 2);
    }


   // memcpy((char *)page + PAGE_SIZE - 2, (void *)&rid.slotNum, 2); // update total number of records;

    memcpy((char *)page + PAGE_SIZE - 4, &freeOffset, 2);    // update free space offset on this page;
    memcpy((char *)page + start, header, 2*fieldSize + 2);

    memcpy((char *)page + start + 2*(1+fieldSize), (char *)data + pointerSize, size -(fieldSize + 1)*2); // update content of the record;
    memcpy((char *)page + PAGE_SIZE - (rid.slotNum + 2)*2, (void *)&start, 2); //update where to begin for this record;

    fileHandle.writePage(rid.pageNum - 1, page);

    free(page);
    free(header);
    return 0;
}*/
/*int getRid( int &size, RID &rid, FileHandle &fileHandle){
	return 0;
}
RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid){
	if(!fileHandle.alreadyOpen()){
		return -1;
	}
	//format null pointer
	void *header = malloc(1600);
	void *page = malloc(PAGE_SIZE);
	int fieldSize = recordDescriptor.size();
	int pointerSize = ceil((double) recordDescriptor.size()/8);
	int size = formatHeader(data, header, recordDescriptor, fieldSize,pointerSize); //this is the size of Record
	if(size<=0) return -1;

	//find the rid and start positon for the record
	short start = getRid(size, rid, fileHandle);

	//memcpy the record
	memcpy((char *)page+start, header, pointerSize);//nullPointer
	//update the directory



}*/
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
	   // short end = start + size;//record ending position
	    short freeOffset = start + size;
	  //  short freesize = PAGE_SIZE - (rid.slotNum + 2)*2 - end; //update free space of this page
	  //cout<<"the free size now is"<<freesize<<endl;

	    fileHandle.readPage(rid.pageNum - 1, page);
	    short N = *(short *)((char *)page + PAGE_SIZE - 2); //append a new page
	    if(rid.slotNum > (unsigned) N){
	    			N = rid.slotNum;

	    }

	    memcpy((char *)page + PAGE_SIZE - 2, &N, 2);

	   // memcpy((char *)page + PAGE_SIZE - 2, (void *)&rid.slotNum, 2); // update total number of records;
	    memcpy((char *)page + PAGE_SIZE - 4, &freeOffset, 2);    // update free space offset on this page;
	        memcpy((char *)page + start, header, 2*fieldSize + 2);

	        memcpy((char *)page + start + 2*(1+fieldSize), (char *)data + pointerSize, size -(fieldSize + 1)*2); // update content of the record;
	        memcpy((char *)page + PAGE_SIZE - (rid.slotNum + 2)*2, (void *)&start, 2); //update where to begin for this record;

	        fileHandle.writePage(rid.pageNum - 1, page);
	        //cout<<"insert into page :" <<  rid.pageNum <<endl;
	        //cout<<"insert into slot :" << rid.slotNum  <<endl;
	        free(page);
	        free(header);
	        return 0;
	    }


RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	int SHIFT = 8;
	//cout<<"read from page :" << rid.pageNum  <<endl;
	//cout<<"read from slot :" << rid.slotNum  <<endl;
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
	//cout<<"here is fine1"<<endl;
	// check if record exist or has been deleted:
	// this is the end offset
	short offSet = *(short *)((char *)page + PAGE_SIZE - (slotNum + 2)*2);
	if(offSet == -1){
		free(page);
		return -1;
	}

	// check if record has been relocated:
	short startPos = offSet;
	if(*(short *)((char *)page + startPos) == -1){
		short newPageNum = *(short *)((char *)page + startPos + 2);
		short newSlotNum = *(short *)((char *)page + startPos + 4);
		RID  newRid;
		newRid.pageNum = newPageNum;
		newRid.slotNum = newSlotNum;
		free(page);
		return readRecord(fileHandle, recordDescriptor,newRid,data);
	}else{
		//de-format to null pointer schema
		//short startPos = 0;
		//startPos = slotNum == 1? 0 : *(short *)((char *)page + PAGE_SIZE - (slotNum + 1)*2);

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
	//cout<<"success 1"<<endl;
	int SHIFT = 8;
	int fieldSize = recordDescriptor.size();
	//cout<<"fieldSize is "<<fieldSize<<endl;
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
    //cout << "success here:" << endl;
    return 0;
}

// Assisting Methods listed below:

short getFreeSpace(void *page) {
	short N = *(short *)((char *)page + PAGE_SIZE - 2);
	short offset = *(short *)((char *)page + PAGE_SIZE - 4) + 2*(N + 2);
	short freeSpace = PAGE_SIZE - offset;
	return freeSpace;
}

// find a slot number for the new record, prerequisite is the space on this page is enough for the record.
RC findAvailableSlot(void *page, unsigned short &slot, short &start) {

	short N = *(short *)((char *)page + PAGE_SIZE - 2);
	start = *(short *)((char *)page + PAGE_SIZE - 4);
	short offset;
	for(short i = 1; i <= N; i++){
		offset = *(short *)((char *)page +PAGE_SIZE-2*(i+2));
		if((offset) == -1) {
			slot = i;
			return 0;
		}
	}
	slot = N+1;
	return 0;
}

//short getOffset(void *page, short N){
//	short freeSpaceOffset = *(short *)((char *)page +PAGE_SIZE-2*(N+2));
//	if(freeSpaceOffset !=-1){
//		return freeSpaceOffset;
//	}else{
//		for(short i=N-1; i>0; i--){
//			if(i==1) return 0;
//			freeSpaceOffset = *(short *)((char *)page + PAGE_SIZE - 2*(i+2));
//			if(freeSpaceOffset==-1) continue;
//			return freeSpaceOffset;
//		}
//	}
//	return -1;
//}
void writeDirectory(FileHandle fileHandle, int pageNum){
	void *page = malloc(PAGE_SIZE);
	RC rc = fileHandle.readPage(pageNum - 1, page);
	short N = 0;
	short F = 0;

	memcpy((char *)page + PAGE_SIZE - 2, &N, 2);

	memcpy((char *)page + PAGE_SIZE - 4, &F, 2);

	fileHandle.writePage(pageNum -1, page);
	free(page);
}

short locatePos(int size, RID &rid, FileHandle &fileHandle){
	int curPage = fileHandle.getNumberOfPages();
	int totalPage = fileHandle.getNumberOfPages();
	unsigned short slot = 1; // initial value for slot number for this new record;
	void *data = malloc(PAGE_SIZE);

	short start = 0;
	if(curPage == 0){ //if entire file is empty
		fileHandle.appendPage(data);
		curPage++;
		writeDirectory(fileHandle, curPage);
		slot = 1;
		start = 0;
	}else{
		fileHandle.readPage(curPage - 1, data);
		short N = *(short *)((char *)data + PAGE_SIZE - 2);
		//start = getOffset(data,N);
		//start = *(int *)((char *)data + PAGE_SIZE - 2*(N + 2));
		//short F = *(short *)((char *)data + PAGE_SIZE - 2*2);
		short freeSpace = getFreeSpace(data);
		if(freeSpace < size + 2){ // need to find page before.
			for(int i = 1; i < curPage; i++){
				fileHandle.readPage(i-1, data);
				N = *(short *)((char *)data + PAGE_SIZE - 2);
				//start = *(int *)((char *)data + PAGE_SIZE - 2 * (N + 2));
				//start = getOffset(data,N);
				//F = *(short *)((char *)data + PAGE_SIZE - 2*2);
				freeSpace = getFreeSpace(data);
				if(N == 0 || freeSpace >= size + 2){
					curPage = i;
					//slot = N+1;
					findAvailableSlot(data, slot, start);
					break;
				}
			}
			if(curPage == totalPage){ //not find any page before current page.

				free(data);
				void *data2 = malloc(PAGE_SIZE);

				int success = fileHandle.appendPage(data2);
				writeDirectory(fileHandle, curPage);
				if(success < 0) return success;
				curPage++;
				slot = 1;
				start = 0;
			}

		}else{ //find space on current page.
			//slot = N + 1;
			//fileHandle.readPage(curPage-1,data);
			findAvailableSlot(data, slot, start);
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
	pointer[0] = 0;

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
	pointer[0] = fieldSize;
	for(int i = 0; i <= fieldSize; i++) {
		memcpy((char *)header + 2*i, &pointer[i], 2);
	}
	return start;
}


//find the record length

short getRecordSize(void *page, const short &start){
	short fieldSize = *(short *)((char *)page + start);
	short recordLen = *(short *)((char *)page + start + 2*fieldSize) + 2*(fieldSize + 1);
	return recordLen;
}

int updateDirectory(void *page, short N, const RID &rid, short dist, short newStart, short newPageOffset, short oldStart){

	if(rid.slotNum > (unsigned) N) return -1;

	memcpy((char *)page + PAGE_SIZE-2*(rid.slotNum+2), (void *)& newStart, 2);

	memcpy((char *)page+PAGE_SIZE-4, (void *)&newPageOffset, 2);

	//if(dist == 0) return 0;

	for (short i = 1; i <= N; i++) {
			//if(i == slotNum) continue;
			short oldOffset = *(short *)((char *)page + PAGE_SIZE - 2*(i+2));

			if (oldOffset == -1 || oldOffset <= oldStart) continue;

			short newOffset = oldOffset - dist;

			memcpy((char *)page + PAGE_SIZE - 2*(i+2), (void *)&newOffset, 2);
			//cout<<"the new offset for the "<<i<<"th slot is"<<newOffset<<endl;
		}

	return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid){
	unsigned int pageNum = rid.pageNum;
	unsigned int slotNum = rid.slotNum;
	if(! fileHandle.alreadyOpen()){
		return -1;
	}
	void *page = malloc(PAGE_SIZE);
	fileHandle.readPage(pageNum-1,page);
	short N = *(short *)((char *)page + PAGE_SIZE - 2);

	//to check the rid is in this page:
	if(slotNum >(unsigned)N){
		free(page);
		return -1;
	}

	//short freeSpaceOffset = getOffset(page,N);
	short freeSpaceOffset = *(short *)((char *)page + PAGE_SIZE-4);
	//cout<<"the freeSpaceOffset at deletion is"<<freeSpaceOffset<<endl;
	//short availableSpace = *(short *)((char *)page + PAGE_SIZE-4);
	//short availableSpace = getFreeSpace(page);

	//check if the record to be deleted has already been deleted:
	short start = *(short *)((char *)page+PAGE_SIZE-2*(slotNum+2));// changed here!!!!!
	if(start==-1){
			free(page);
			return -1;
		}


	//find the start point
//	short start = 0;
//	for(short i = slotNum; i >= 1; i--) {
//			if(i == 1){
//				start = 0;
//			}else{
//				start = *(short *)((char *)page + PAGE_SIZE - (i+1)*2);
//				if(start != -1) break;
//			}
//		}
	short end = start + getRecordSize(page, start);

	short isRedirected = *(short *)((char *)page + start);
	if(isRedirected == -1){
		short newPageNum = *(short *)((char *)page + start + 2);
		short newSlotNum = *(short *)((char *)page + start + 4);
		RID  newRid;
		newRid.pageNum = newPageNum;
		newRid.slotNum = newSlotNum;
		free(page);
		return deleteRecord(fileHandle, recordDescriptor,newRid);
	}

	//move the follow part forward by end-start
	memmove((char *)page + start,(char *)page + end, freeSpaceOffset-end);
	//update the directory
	//directoryUpdate(page, N,rid,end-start, -1,availableSpace+(end-start));
	short distance = end - start;
	freeSpaceOffset -= distance;
	updateDirectory(page, N, rid, distance, -1, freeSpaceOffset, start);

	RC rc = fileHandle.writePage(pageNum-1,page);
	if(rc!=0){
		free(page);
		return rc;
	}
	free(page);
	return 0;
}



RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid){


	unsigned int pageNum = rid.pageNum;
	unsigned int slotNum = rid.slotNum;
	if(!fileHandle.alreadyOpen()){
		return -1;
	}
	void *page = malloc(PAGE_SIZE);
	//void *oldR = malloc(2000);
	fileHandle.readPage(pageNum-1,page);
	short N = *(short *)((char *)page + PAGE_SIZE - 2);
	//short freeSpaceOffset = getOffset(page,N);
	//short availableSpace = *(short *)((char *)page + PAGE_SIZE-4);
	short freeSpaceOffset = *(short *)((char *)page + PAGE_SIZE - 4);
	short availableSpace = getFreeSpace(page);
	short start = *(short *)((char *)page+PAGE_SIZE-2*(slotNum+2));

	short end = start + getRecordSize(page, start);

	if(slotNum > (unsigned) N || start == -1){
		free(page);
		return -1;

		}

//	short start = 0;
//	for(short i=slotNum; i>0; i--){
//		if(i==1){
//			start=0;
//		}else{
//			start = *(short *)((char *)page+PAGE_SIZE-2*(i+1));
//			if(start != -1) break;
//		}
//	}
	//the old record is redirected
	short isRedirected = *(short *)((char *)page + start);
	if(isRedirected==-1){
		short newPageNum = *(short *)((char *)page + start + 2);
		short newSlotNum = *(short *)((char *)page + start + 4);
		RID  newRid;
		newRid.pageNum = newPageNum;
		newRid.slotNum = newSlotNum;
		free(page);
		return updateRecord(fileHandle, recordDescriptor,data, newRid);
	}

	void *header = malloc(1600);
	int newFieldSize = recordDescriptor.size();
	int newPointerSize = ceil((double) recordDescriptor.size() / 8);
	int newDataLen = formatHeader(data, header, recordDescriptor, newFieldSize, newPointerSize);
	//if the current page is enough for the update
	if(availableSpace-newDataLen+(end-start)>=0){
		memmove((char *)page + start + newDataLen, (char *)page + end, freeSpaceOffset - end);
		memcpy((char *)page + start,header, 2*(newFieldSize+1));
		memcpy((char *)page + start + 2*(1+newFieldSize), (char *)data + newPointerSize, newDataLen -(newFieldSize + 1)*2);

		//update the directory
		//short distance = end - start - newDataLen;
		freeSpaceOffset = freeSpaceOffset + newDataLen - (end - start);

		//updateDirectory(void *page, const short &N, const RID &rid, const short &dist, const short &newStart, const short &newPageOffset, const short &oldStart)
		//if((end - start) != newDataLen) {

		updateDirectory(page, N, rid, end-start - newDataLen, start, freeSpaceOffset, start);
		//}
		//directoryUpdate(page, N, rid,end-start-newDataLen, start+newDataLen,availableSpace-newDataLen+(end-start));
	}else{
		//find a new page to insert the data, change the old data to redirection info
		RID newRid;
		RC rc = insertRecord(fileHandle, recordDescriptor, data, newRid);
		if(rc==-1){
			free(page);
			return rc;
		}
		memmove((char *)page+start+2*(2 + 1), (char *)page+end, freeSpaceOffset-end);
		memset((char *)page+start, -1, 2);
		short newPage = newRid.pageNum;
		short newSlot = newRid.slotNum;
		memcpy((char *)page+start+2, (void *)&newPage, 2);
		memcpy((char *)page+start+4, (void *)&newSlot, 2);
		// update the directory
		short distance = end - start - 2 * (2 + 1);
		freeSpaceOffset -= distance;
		updateDirectory(page, N, rid, distance, start, freeSpaceOffset, start);
		//directoryUpdate(page, N, rid,end-start-6, start+6,availableSpace-6+(end-start));
	}

	fileHandle.writePage(pageNum-1, page);
	free(page);
	free(header);
	return 0;
}


// need to check if need to include null pointer

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data){
	if(! fileHandle.alreadyOpen()) {
		return -1;
	}
	void *page = malloc(PAGE_SIZE);
	int rc1 = readRecord(fileHandle, recordDescriptor, rid, page);
	if(rc1 < 0){
		return -1;
	}
	int pointerSize = ceil((double) recordDescriptor.size() / 8);
	int fieldSize = recordDescriptor.size();
	memcpy((char *)data, page, pointerSize);

	int start = pointerSize;
	for(int i = 0; i < fieldSize; i++){
		Attribute attribute = recordDescriptor[i];
		if(attribute.name != attributeName) {
			int oldStart = start;
			start += attribute.type == TypeVarChar ? *(int *)((char *)page + oldStart) + 4 : 4;
		}else{
			if(attribute.type == TypeVarChar) {
				memcpy((char *)data + pointerSize, (char *)page + start, 4 + *(int *)((char *)page + start));
			}else{
				memcpy((char *)data + pointerSize, (char *)page + start, 4);
			}
			break;
		}
	}
	free(page);


	return 0;
}


RC RBFM_ScanIterator::initializeSI( void *page,FileHandle &fileHandle,
		  	  const vector<Attribute> &recordDescriptor,
		  	  const string conditionAttribute,
			  const CompOp compOp,
			  const void *value,
			  const vector<string> &attributeNames){

	this->fileHandle = fileHandle;
	this->currentRid.pageNum=1;
	this->currentRid.slotNum=1;
	this->recordDescriptor = recordDescriptor;
	this->conditionAttribute = conditionAttribute;
	this->compOp = compOp;
	this->value = value;
	this->attributeNames = attributeNames;
	this->page = page;
	return 0;
}

bool passComp(const void *field, const CompOp compOp, const void *value, const AttrType attrType){
	if(attrType == TypeInt){
		int intField = *(int *)field;
		int intValue = *(int *)value;
		switch(compOp){
		case 0: //=
			return (intField == intValue);
			break;
		case 1://<
			return (intField < intValue);
			break;
		case 2://<=
			return (intField <= intValue);
			break;
		case 3://>
			return (intField > intValue);
			break;
		case 4://>=
			return (intField >= intValue);
			break;
		case 5://!=
			return (intField != intValue);
			break;
		case 6://no compOp
			return true;
			break;
		default:
			return true;
		}
	}
	else if (attrType == TypeReal){
		float floatField = *(float *)field;
		float floatValue = *(float *)value;
		switch(compOp){
			case 0: //=
				return (floatField == floatValue);
				break;
			case 1://<
				return (floatField < floatValue);
				break;
			case 2://<=
				return (floatField <= floatValue);
				break;
			case 3://>
				return (floatField > floatValue);
				break;
			case 4://>=
				return (floatField >= floatValue);
				break;
			case 5://!=
				return (floatField != floatValue);
				break;
			case 6://no compOp
				return true;
				break;
			default:
				return true;
		}
	}else{
		int strLen1 = *(int *)((char *)field);
		string fieldval;
		for (int i = 0; i < strLen1; i++)
			{
				fieldval.push_back(*(char *)((char *)field+4+i));
			}
		int strLen2 = *(int *)((char *)value);
		string compval;
		for (int i = 0; i < strLen2; i++)
			{
				compval.push_back(*(char *)((char *)value+4+i));
			}
		//cout<<"fieldval is "<<fieldval.c_str()<<"   compval is "<<compval.c_str()<<endl;
		switch(compOp){
		case 0://=
			return (strcmp(fieldval.c_str(),compval.c_str())==0);
			break;
		case 1: //<
			return (strcmp(fieldval.c_str(),compval.c_str())<0);
			break;
		case 2://<=
			return (strcmp(fieldval.c_str(),compval.c_str())<=0);
			break;
		case 3://>
			return (strcmp(fieldval.c_str(),compval.c_str())>0);
			break;
		case 4://>=
			return (strcmp(fieldval.c_str(),compval.c_str())>=0);
			break;
		case 5://!=
			return (strcmp(fieldval.c_str(),compval.c_str())==0);
			break;
		case 6://no compOp
			return true;
			break;
		default:
			return true;
		}
	}
}
RC RecordBasedFileManager::scan(FileHandle &fileHandle,
	const vector<Attribute> &recordDescriptor,
    const string &conditionAttribute,
    const CompOp compOp,                  // comparision type such as "<" and "="
    const void *value,                    // used in the comparison
    const vector<string> &attributeNames, // a list of projected attributes
    RBFM_ScanIterator &rbfm_ScanIterator) {
	RC rc1 = fileHandle.alreadyOpen();
	int a = fileHandle.getNumberOfPages();
	//cout<<"the number of pages "<<a<<endl;
	if(!fileHandle.alreadyOpen() || fileHandle.getNumberOfPages()== 0) {
		return -1;
	}

	void *page = malloc(PAGE_SIZE);
	RC rc = fileHandle.readPage(0,page);
	if(rc==-1){
		free(page);
		return -1;
	}

	if(conditionAttribute == ""){
		if (compOp != NO_OP) {
			return -1;
		}
	}
	return rbfm_ScanIterator.initializeSI(page,fileHandle, recordDescriptor, conditionAttribute,compOp,value,attributeNames);
}


RC reformRecord(const vector<string> &attributeNames,
		const vector<Attribute> &recordDescriptor,
		void *oldRecord,
		void *data){
	//this function returns the right formated data
	int SHIFT = 8;
	int newpointerSize = ceil((double)attributeNames.size()/SHIFT);
	int newFieldSize = attributeNames.size();
	int oldFieldSize = recordDescriptor.size();
	int nullArray[newFieldSize];
	short newOffset=0;
	void *tempRecord = malloc(1000);
	//get the data part ready
	for(int i=0; i<newFieldSize; i++){

		int j = 0;
		for(; j< oldFieldSize; j++){

			if(recordDescriptor[j].name == attributeNames[i]){
				//we find the field, cpy it to tempRecord, and update the null pointer
				short fieldStart = -1;
				if(j==0){
					fieldStart = 0;
				}else{
					fieldStart = *(short *)((char *)oldRecord+ j*2);
				}
				short fieldEnd = *(short *)((char *)oldRecord + (j+1)*2);

				memcpy((char *)tempRecord+newOffset, (char *)oldRecord+2*(1+oldFieldSize)+fieldStart,fieldEnd-fieldStart);
				newOffset += (fieldEnd- fieldStart);
				if(fieldStart==fieldEnd){
					nullArray[i]=1;
				}else{
					nullArray[i]=0;
				}
				break;
			}
		}
		if(j == recordDescriptor.size()) return -1;
	}
	//get the null pointer ready
	unsigned char *nullPointer = (unsigned char *)malloc(newpointerSize);
	for(int i = 0; i < newpointerSize; i++) {
		int sum = 0;
	    int j = SHIFT*i;
	    while(j < (i+1)*SHIFT && j < newFieldSize){
	    		sum |= nullArray[j]*(1 << (SHIFT -1 - j%SHIFT));
	        j++;
	    }
	    *(nullPointer + i) = sum;
	}
	memcpy((char *)data, nullPointer, newpointerSize);
	memcpy((char *) data + newpointerSize, (char *)tempRecord, newOffset);

	free(nullPointer);
	free(tempRecord);

	return 0;
}


//scan if the attribute type is int, no need to add null pointer.
//
/*
RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data){
	bool isRightRecord = false;
  	int totalPage = fileHandle.getNumberOfPages();
	short N = *(short *)((char *)page+PAGE_SIZE-2);
	cout<<"total page number is "<<totalPage<<endl;
	cout<<"total records number is "<<N<<endl;
		if(currentRid.slotNum > N){

		if(currentRid.pageNum >= totalPage){
			return RBFM_EOF;

		}else{
			cout<<"page number will increase by one at start"<<endl;

			currentRid.pageNum += 1;
			currentRid.slotNum = 1;
			fileHandle.readPage(currentRid.pageNum - 1,page);

			return getNextRecord(rid,data);

		}
		}
	if(conditionAttribute == ""){
		cout<<"the condition attribute is empty"<<endl;
	short start = *(short *)((char *)page+PAGE_SIZE-2*(currentRid.slotNum+2));
	if(start == -1){
		currentRid.slotNum +=1;
		return getNextRecord(rid,data);
		}
	short isRedirected = *(short *)((char *)page+start);
	if(isRedirected==-1){
			currentRid.slotNum +=1;
			return getNextRecord(rid,data);
	}
	void *oldRecord1 = malloc(2000);

	short oldStart = *(short *)((char *)page+PAGE_SIZE-2*(currentRid.slotNum+2));
	short oldEnd = oldStart + getRecordSize(page, oldStart);
	memcpy(oldRecord1,(char *)page+oldStart,oldEnd-oldStart);

	reformRecord(attributeNames,recordDescriptor,oldRecord1, data);
	rid = currentRid;
	currentRid.slotNum += 1;
	free(oldRecord1);
	return 0;
	}
	else{
		cout<<"this should not appear"<<endl;
		short fieldLoc = -1; //condition attribute location
		int fieldLen = -1;
		for(int i=0; i<recordDescriptor.size();i++){
			if(recordDescriptor[i].name == conditionAttribute){
				fieldLoc = i+1;
				fieldLen = recordDescriptor[i].length;
				break;
			}
		}
		if(fieldLoc == -1) return RBFM_EOF;
		int i=0;
		for(i=currentRid.slotNum ; i<=N; i++){

			short start = *(short *)((char *)page+PAGE_SIZE-2*(i+2));

			if(start == -1){
				continue;
			}
			short isRedirected = *(short *)((char *)page+start);
			if(isRedirected==-1){
					continue;
			}
			//find the specific field in the record
		short fieldEnd = *(short *)((char *)page+start+2*fieldLoc);
		short fieldStart = (fieldLoc==1) ? 0 : *(short *)((char *)page+start+2*(fieldLoc-1));
		if(fieldEnd == fieldStart){
			continue;
		}
		void *field = malloc(fieldLen);
		memcpy(field, (char *)page+start+fieldStart+2*(recordDescriptor.size()+1), fieldEnd-fieldStart);
		AttrType attrType;
		attrType = recordDescriptor[fieldLoc-1].type;
		isRightRecord = passComp(field, compOp, value, attrType); //bypass when attribute is empty;

		free(field);
		if(isRightRecord==false){
			continue;
		} else{
			currentRid.slotNum =i;
			break;
			}
	}
	if(i == N+1){
			if(currentRid.pageNum == totalPage) return RBFM_EOF;
			else{
				fileHandle.readPage(currentRid.pageNum,page);
				cout<<"page number increases by 1 at end"<<endl;
				currentRid.pageNum +=1;
				currentRid.slotNum = 1;
				//free(page);
				return getNextRecord(rid, data);
			}
		}
		void *oldRecord = malloc(2000);

	short oldStart = *(short *)((char *)page+PAGE_SIZE-2*(currentRid.slotNum+2));
	short oldEnd = oldStart + getRecordSize(page, oldStart);
	memcpy(oldRecord,(char *)page+oldStart,oldEnd-oldStart);

   //here need to be modified

	reformRecord(attributeNames,recordDescriptor,oldRecord, data);
	//cout<<"the record we found is"<<endl;
	rid = currentRid;
	currentRid.slotNum += 1;
	free(oldRecord);
	return 0;

 }
 }

*/
RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data){
	//find the attribute that satisfies conditionAttribute

	//cout<<"the current page is "<<currentRid.pageNum<<endl;
	int totalPage = fileHandle.getNumberOfPages();
	short N = *(short *)((char *)page+PAGE_SIZE-2);

	//check the current rid is valid or not
	if(currentRid.slotNum > N){

		if(currentRid.pageNum >= totalPage){
			return RBFM_EOF;

		}else{

			currentRid.pageNum += 1;
			currentRid.slotNum = 1;
			fileHandle.readPage(currentRid.pageNum - 1,page);

			return getNextRecord(rid,data);

		}
	}

	//read the page, and scan
	//fileHandle.readPage(currentRid.pageNum-1,page);
	short fieldLoc = -1; //condition attribute location
	int fieldLen = -1; //condition attribute length, 4 for type int and float; 50 for type varchar
	bool isRightRecord = false;
	bool bypassComp = false;
	//when the conditionAttribute is empty, just assume the current record is the right one
	if(conditionAttribute ==""){
		//cout<<"we get the condition attribute is empty"<<endl;
		fieldLoc = 1;
		fieldLen = recordDescriptor[0].length;
		isRightRecord = true;
		bypassComp = true;

	}else{ //if ca is not empty
		//cout<<"this should be implemented"<<"the condition attribute is "<<
		//		conditionAttribute<<endl;

		for(int i=0; i<recordDescriptor.size();i++){
			if(recordDescriptor[i].name == conditionAttribute){
				fieldLoc = i+1;
				fieldLen = recordDescriptor[i].length;
				//cout<<"the filedLoc is "<<fieldLoc<<endl;
				break;
			}
		}
	}
	if(fieldLoc == -1) return RBFM_EOF;
	    //start from the current slotNum, find the next record
	int i=0;
	for(i=currentRid.slotNum ; i<=N; i++){

		short start = *(short *)((char *)page+PAGE_SIZE-2*(i+2));
		//cout<<"the start is "<<start<<endl;
		if(start == -1){
				continue;
		}
			//short end = start + getRecordSize(page, start);
		short isRedirected = *(short *)((char *)page+start);
		if(isRedirected==-1){
				continue;
		}
			//find the specific field in the record
		if(!bypassComp) {
		//cout<<"line 1052 should not be implemented"<<endl;
		short fieldEnd = *(short *)((char *)page+start+2*fieldLoc);
		short fieldStart = (fieldLoc==1) ? 0 : *(short *)((char *)page+start+2*(fieldLoc-1));
		if(fieldEnd == fieldStart){
			continue;
		}
		//cout<<"here is fine 11111"<<endl;
		void *field = malloc(fieldLen);
		memcpy(field, (char *)page+start+fieldStart+2*(recordDescriptor.size()+1), fieldEnd-fieldStart);

			//check if satisfies compOp
		AttrType attrType;
		attrType = recordDescriptor[fieldLoc-1].type;
	//	if( !isRightRecord){
			isRightRecord = passComp(field, compOp, value, attrType); //bypass when attribute is empty;
//		}

		free(field);
		//cout<<"here is fine 222"<<endl;
		}
			//if not satisfies, continue;

		if(isRightRecord==false){
			//cout<<"line 1075 should not be implemented"<<endl;
			continue;
		} else{
			//this is the right record, re-format the record and get the right rid and data
			currentRid.slotNum =i;
			//cut the record according to the attributeNames
			break;
			}
		}
		//when we finish the current page but don't find the record
		if(i == N+1){
			if(currentRid.pageNum == totalPage) return RBFM_EOF;
			else{
				fileHandle.readPage(currentRid.pageNum,page);
				currentRid.pageNum +=1;
				currentRid.slotNum = 1;
				//free(page);
				return getNextRecord(rid, data);
			}
		}

	//re-format the Record
	void *oldRecord = malloc(2000);

	short oldStart = *(short *)((char *)page+PAGE_SIZE-2*(currentRid.slotNum+2));
	short oldEnd = oldStart + getRecordSize(page, oldStart);
	memcpy(oldRecord,(char *)page+oldStart,oldEnd-oldStart);

   //here need to be modified

	reformRecord(attributeNames,recordDescriptor,oldRecord, data);



	rid = currentRid;
	currentRid.slotNum += 1;
	free(oldRecord);
	return 0;
}


/*RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data){
	//find the attribute that satisfies conditionAttribute


	//int totalPage = fileHandle.getNumberOfPages();
	short N = *(short *)((char *)page+PAGE_SIZE-2);
	short totalPage = fileHandle.getNumberOfPages();
	//check the current rid is valid or not
	if(currentRid.slotNum > (unsigned) N){

		if(currentRid.pageNum >= (unsigned) totalPage){
			return RBFM_EOF;
		}
		currentRid.pageNum += 1;
				currentRid.slotNum = 1;
				fileHandle.readPage(currentRid.pageNum - 1,page);

				return getNextRecord(rid,data);


			}

			//read the page, and scan
			//fileHandle.readPage(currentRid.pageNum-1,page);
			short fieldLoc = -1; //condition attribute location
			int fieldLen = -1; //condition attribute length, 4 for type int and float; 50 for type varchar
			bool isRightRecord = false;
			bool bypassComp = false;
			//when the conditionAttribute is empty, just assume the current record is the right one
				if(conditionAttribute ==""){

					fieldLoc = 1;
					fieldLen = 100;
					isRightRecord = true;
					bypassComp = true;

				}else{ //if ca is not empty
					for(int i=0; i<recordDescriptor.size();i++){
						if(recordDescriptor[i].name == conditionAttribute){
							fieldLoc = i+1;
							fieldLen = recordDescriptor[i].length;
							break;
						}
					}
				}
				if(fieldLoc == -1) return RBFM_EOF;
					    //start from the current slotNum, find the next record

					int i = currentRid.slotNum;
					for(; i<=N; i++){
						//cout<<"the current slotNum is "<<i<<endl;
							//find the record with the condition attribute
							//short end = *(short *)((char *)page+PAGE_SIZE-(i+2)*2);
							//if(end==-1){
							//	continue;
							//}
							//short start = getStart(page,i);
						short start = *(short *)((char *)page+PAGE_SIZE-2*(i+2));
						//cout<<"the start is "<<start<<endl;
						if(start == -1){
								continue;
						}
						//short end = start + getRecordSize(page, start);
								short isRedirected = *(short *)((char *)page+start);
								if(isRedirected==-1){
										continue;
								}
									//find the specific field in the record
								if(! bypassComp) {
								short fieldEnd = *(short *)((char *)page + start + 2*fieldLoc);
								short fieldStart = (fieldLoc==1) ? 0 : *(short *)((char *)page+start+2*(fieldLoc-1));
								if(fieldEnd == fieldStart){
									continue;
								}
								void *field = malloc(fieldLen);
								memcpy(field, (char *)page+start+fieldStart+2*(recordDescriptor.size()+1), fieldEnd-fieldStart);

								AttrType attrType;
										attrType = recordDescriptor[fieldLoc-1].type;
								//		if( !isRightRecord){
										isRightRecord = passComp(field, compOp, value, attrType); //bypass when attribute is empty;
								//		}

										free(field);
										}
											//if not satisfies, continue;

										if(isRightRecord == false){
											continue;
										}
											//this is the right record, re-format the record and get the right rid and data
											//cout<<"we have found the right record!"<<endl;
										currentRid.slotNum =i;
													//cut the record according to the attributeNames
													//re-format the Record
													void *oldRecord = malloc(2000);

													short oldStart = *(short *)((char *)page+PAGE_SIZE-2*(currentRid.slotNum+2));
													short oldEnd = oldStart + getRecordSize(page, oldStart);
													memcpy(oldRecord,(char *)page+oldStart,oldEnd-oldStart);

												   //here need to be modified

													RC rc = reformRecord(attributeNames,recordDescriptor,oldRecord, data);
													if(rc < 0) return RBFM_EOF;

													rid.pageNum = currentRid.pageNum;
																rid.slotNum = currentRid.slotNum;

																currentRid.slotNum += 1;
																free(oldRecord);
																return 0;
																//break;

															}
															//when we finish the current page but don't find the record
															if((short) i == N+1){
																if(currentRid.pageNum == (unsigned) totalPage) return RBFM_EOF;
																else{

																	currentRid.pageNum +=1;
																	currentRid.slotNum = 1;
																	fileHandle.readPage(currentRid.pageNum - 1,page);
																					//free(page);
																					return getNextRecord(rid, data);
																				}
																			}
																		return RBFM_EOF;

																	}


*/
