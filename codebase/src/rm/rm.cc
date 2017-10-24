
#include "rm.h"

RelationManager* RelationManager::instance()
{
    static RelationManager _rm;
    return &_rm;
}

RelationManager::RelationManager()
{

}

RelationManager::~RelationManager()
{
}

int getNextID(int &nextID) {
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RBFM_ScanIterator rbfmsi ;
	FileHandle fileHandle;
	rbfm->openFile(TABLE,fileHandle);
	vector<Attribute> tableDescriptor;
	prepareAttribute4Table(tableDescriptor);

	vector<string> conditionAttribute;
	conditionAttribute.push_back("table-id");
	void *value = NULL;
	RC rc=rbfm->scan(fileHandle,tableDescriptor,"",NO_OP,value,conditionAttribute,rbfmsi);
	if(rc!=0){
		rbfmsi.close();
		rbfm->closeFile(fileHandle);
		return rc;
	}
	int currentID = -1;
	int maxID = 1;
	RID rid;
	void *data = malloc(50);
	while (rbfmsi.getNextRecord(rid, data) != RBFM_EOF) {
			currentID = *(int *)((char *)data + 1);
			if (currentID>maxID){
				maxID = currentID;
			}
		}
	nextID = currentID+1;
	rbfmsi.close();
	rbfm->closeFile(fileHandle);
	free(data);
	return 0;
}

void prepareAttribute4Table(vector<Attribute> &tableDescriptor) {
	Attribute table_id, table_name, file_name;
	table_id.name = "table-id";
	table_id.type = TypeInt;
	table_id.length = sizeof(int);

	table_name.name = "table-name";
	table_name.type = TypeVarChar;
	table_name.length = 50;

	file_name.name = "file-name";
	file_name.type = TypeVarChar;
	file_name.length = 50;

	//tableDescriptor = {};
	tableDescriptor.push_back(table_id);
	tableDescriptor.push_back(table_name);
	tableDescriptor.push_back(file_name);
}

void prepareAttribute4Column(vector<Attribute> &columnDescriptor) {
	Attribute table_id, column_name, column_type, column_length, column_position;

	table_id.name = "table-id";
	table_id.type = TypeInt;
	table_id.length = sizeof(int);

	column_name.name = "column-name";
	column_name.type = TypeVarChar;
	column_name.length = 50;

	column_type.name = "column-type";
	column_type.type = TypeInt;
	column_type.length = sizeof(int);

	column_length.name = "column-length";
	column_length.type = TypeInt;
	column_length.length = sizeof(int);

	column_position.name = "column-position";
	column_position.type = TypeInt;
	column_position.length = sizeof(int);

	//columnDescriptor = {};
	columnDescriptor.push_back(table_id);
	columnDescriptor.push_back(column_name);
	columnDescriptor.push_back(column_type);
	columnDescriptor.push_back(column_length);
	columnDescriptor.push_back(column_position);

}

void prepareRecord4Tables(const int tid, const char *tableName, const int tlen, const char *fileName,
		const int flen, const int pointerSize, void *data) {
	unsigned char *nullPointer = (unsigned char *)malloc(pointerSize);
	memset(nullPointer, 0, pointerSize);
	memcpy((char *)data, nullPointer, pointerSize);
	int start = pointerSize;
	memcpy((char *)data + start, &tid, 4);
	start += 4;
	memcpy((char *)data + start, &tlen, 4);
	start += 4;
	memcpy((char *)data + start, tableName, tlen);
	start += tlen;
	memcpy((char *)data + start, &flen, 4);
	start += 4;
	memcpy((char *)data + start, fileName, flen);
	start += flen;
	free(nullPointer);

}
//prepareRecord4Columns(tid, attri.name.c_str(), attri.name.size(), attri.type, attri.length, i, nullPointerSize, buffer);
void prepareRecord4Columns(const int cid, const char* columnName, const int clen, AttrType type, const int len, const int pos, const int pointerSize, void * data) {
	unsigned char* nullPointer = (unsigned char *)malloc(pointerSize);
	memset(nullPointer, 0, pointerSize);
	memcpy((char *)data, nullPointer, pointerSize);
	int start = pointerSize;
	memcpy((char *)data + start, &cid, 4);
	start += 4;
	memcpy((char *)data + start, &clen, 4);
	start += 4;
	memcpy((char *)data + start, columnName, clen);
	start += clen;
	memcpy((char *)data + start, &type, 4);
	start += 4;
	memcpy((char *)data + start, &len, 4);
	start += 4;
	memcpy((char *)data + start, &pos, 4);
	start += 4;
	free(nullPointer);

}

bool tableNameOccuppied(const string  & tableName){
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RBFM_ScanIterator rbfmsi;
	FileHandle fileHandle;
	rbfm -> openFile(TABLE, fileHandle);
	vector<Attribute> tableDescriptor;
	prepareAttribute4Table(tableDescriptor);
	string conditionAttribute = "table-name";
	void *value = malloc(54);
	int nameSize = tableName.size();
	memcpy(value,(void *)&nameSize,4);
	memcpy((char *)value+sizeof(int), tableName.c_str(), nameSize);
	vector<string> attributeNames;
	attributeNames.push_back("table-id");

	//TODO****//
	RID rid;
	void *data = malloc(100);
	rbfm->scan(fileHandle, tableDescriptor, conditionAttribute,EQ_OP,value, attributeNames,rbfmsi);
	while(rbfmsi.getNextRecord(rid, data)!= -1){
		rbfmsi.close();
		rbfm->closeFile(fileHandle);
		free(data);
		free(value);
		return true;
	}
	rbfmsi.close();
	rbfm->closeFile(fileHandle);
	free(data);
	free(value);
	return false;
}

int callInsertRecord(const string &fileName, const vector<Attribute>& descriptor, const void *data, RID &rid) {
	FileHandle fileHandle;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc1 = rbfm -> openFile(fileName, fileHandle);
	if(rc1 < 0) {
		return rc1;
	}
	RC rc2 = rbfm -> insertRecord(fileHandle, descriptor, data, rid);
	rbfm -> closeFile(fileHandle);
	return rc2;
}

int callDeleteRecord(const string &fileName, const vector<Attribute>& descriptor, RID &rid) {
	FileHandle fileHandle;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc1 = rbfm -> openFile(fileName, fileHandle);
	if(rc1 < 0) {
			return rc1;
	}
	RC rc2 = rbfm -> deleteRecord(fileHandle, descriptor, rid);

	rbfm -> closeFile(fileHandle);

	return rc2;
}

RC RelationManager::createCatalog()
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc1 = rbfm->createFile(TABLE);
	RC rc2 = rbfm->createFile(COLUMN);
	if(rc1 < 0 || rc2 < 0){
		return -1;
	}

	int tid = 1;
	int cid = 2;
	// create Tables:
	FileHandle tableHandle;
	rbfm -> openFile(TABLE, tableHandle); // open Table to insert table, column;
	void *buffer = malloc(100);
	vector<Attribute> tableDescriptor;
	prepareAttribute4Table(tableDescriptor);
	int pointerSize = ceil((double) tableDescriptor.size() / 8);

	prepareRecord4Tables(tid, TABLE, 6, TABLE, 6, pointerSize, buffer);
	RID rid1;
	rbfm->insertRecord(tableHandle, tableDescriptor, buffer, rid1);
	free(buffer);

	buffer = malloc(100);
	//int pointerSize4C = ceil((double) columnDescriptor.size() / 8);
	prepareRecord4Tables(cid, COLUMN, 7, COLUMN, 7, pointerSize, buffer);
	RID rid2;
	rbfm -> insertRecord(tableHandle, tableDescriptor, buffer, rid2);
	free(buffer);

	rbfm -> closeFile(tableHandle);

	// create Columns:
	vector<Attribute> columDescriptor;
	prepareAttribute4Column(columDescriptor);

	FileHandle columnHandle;
	rbfm -> openFile(COLUMN, columnHandle);
	vector<Attribute> columnDescriptor;
	prepareAttribute4Column(columnDescriptor);
	int nullPointerSize = ceil((double) columnDescriptor.size() / 8);
	for(int i = 1; i <= tableDescriptor.size(); i++){
		Attribute attri = tableDescriptor[i-1];
		buffer = malloc(100);
		prepareRecord4Columns(tid, attri.name.c_str(), attri.name.size(), attri.type, attri.length, i, nullPointerSize, buffer);
		RID rid;
		rbfm -> insertRecord(columnHandle, columnDescriptor, buffer, rid);
		free(buffer);
	}

	for(int i = 1; i <= columnDescriptor.size(); i++) {
		Attribute attri2 = columnDescriptor[i-1];
		buffer = malloc(100);
		prepareRecord4Columns(cid, attri2.name.c_str(), attri2.name.size(), attri2.type, attri2.length, i, nullPointerSize, buffer);
		RID rid;
		rbfm -> insertRecord(columnHandle, columnDescriptor, buffer, rid);
		free(buffer);
	}

	rbfm -> closeFile(columnHandle);

	return 0;
}

RC RelationManager::deleteCatalog()
{
    RecordBasedFileManager *rbfm = RecordBasedFileManager:: instance();
    RC rc1 = rbfm -> destroyFile(TABLE);
    RC rc2 = rbfm -> destroyFile(COLUMN);
    if(rc1 < 0 || rc2 < 0) {
    	 return -1;
    }
    return 0;
}

/*RC RelationManager::getNextTableId(RecordBasedFileManager *rbfm, int &nextTableId){
	FileHandle fileHandle;
	rbfm->openFile(TABLE,fileHandle);
	vector<Attribute> tableDescriptor;
	prepareAttribute4Table(tableDescriptor);

	return 0;
}*/

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{

	// check if tableName already exist in TABLE;
	if(tableNameOccuppied(tableName)){
		return -1;
	}
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	// check tableName file already exist:
	int rc1 = rbfm -> createFile(tableName);
	if(rc1 < 0) return -1;

	// allocate a new ID in TABLE/COLUMN for new tableName to occupy:
	int newID = -1;
	getNextID(newID);

	void *buffer = malloc(100);

	//insert into TABLE:
	vector<Attribute> tableDescriptor;
	prepareAttribute4Table(tableDescriptor);
	int pointerSize = ceil((double) tableDescriptor.size() / 8);

	prepareRecord4Tables(newID, tableName.c_str(), tableName.size(), tableName.c_str(), tableName.size(), pointerSize, buffer);
	RID rid1;
	RC rc2 = callInsertRecord(TABLE, tableDescriptor, buffer, rid1);
	free(buffer);
	if(rc2 < 0) return rc2;


	//insert into COLUMN:
	vector<Attribute> columnDescriptor;
	prepareAttribute4Column(columnDescriptor);
	int columnPointerSize = ceil((double) columnDescriptor.size() / 8);
	for(int i = 1; i <= attrs.size(); i++) {
		Attribute attribute = attrs[i-1];
		buffer = malloc(100);
		prepareRecord4Columns(newID, attribute.name.c_str(), attribute.name.size(), attribute.type, attribute.length, i, columnPointerSize, buffer);
		RID rid_i;
		RC rci = callInsertRecord(COLUMN, columnDescriptor, buffer, rid_i);
		free(buffer);
		if(rci < 0) return rci;
	}

    return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
	if(tableName == TABLE || tableName == COLUMN) return -1;

	//get the parameters for scan ready
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RBFM_ScanIterator rbfmsi;
	FileHandle fileHandleT;
	rbfm->openFile(TABLE,fileHandleT);
	vector<Attribute> tableDescriptor;
	prepareAttribute4Table(tableDescriptor);
	string conditionAttribute1 = "table-name";
	void *value1 = malloc(54);
	int nameSize = tableName.size();
	memcpy(value1, &nameSize, 4);
	memcpy((char *)value1+4,tableName.c_str(),nameSize);
	vector<string> attributeNames1;
	attributeNames1.push_back("table-id");
	attributeNames1.push_back("file-name");

	//scan TABLE and find the table,get the table-id&filename
	RC rc=rbfm->scan(fileHandleT,tableDescriptor,conditionAttribute1,EQ_OP,value1,attributeNames1,rbfmsi);
	if(rc!=0){
		free(value1);
		rbfmsi.close();
		rbfm->closeFile(fileHandleT);
		return rc;
	}
	RID trid;
	void *tableRecord = malloc(200);
	//delete the record in the TABLE
	rc = rbfmsi.getNextRecord(trid,tableRecord);
	if(rc!=0){
		free(value1);
		free(tableRecord);
		rbfmsi.close();
		rbfm->closeFile(fileHandleT);
		return -1;
	}
	rbfm->deleteRecord(fileHandleT,tableDescriptor,trid);

	int tableID = *(int *)((char *)tableRecord+1);
	free(value1);
	free(tableRecord);
	//get the parameters ready for scan the COLUMN
	FileHandle fileHandleC;
	rbfm->openFile(COLUMN,fileHandleC);
	vector<Attribute> columnDescriptor;
	prepareAttribute4Column(columnDescriptor);
	string conditionAttribute2 = "table-id";
	void *value2 = malloc(4);
	memcpy(value2,&tableID,4);
	vector<string> attributeNames2;
	attributeNames2.push_back("table-id");

	//scan COLUMN by table-id, get the columns of the table
	rc= rbfm->scan(fileHandleC,columnDescriptor,conditionAttribute2,EQ_OP,value2,attributeNames2,rbfmsi);
	if(rc!=0){
		free(value2);
		rbfmsi.close();
		rbfm->closeFile(fileHandleC);
		return rc;
	}
	//delete all the records in COLUMN
	RID crid;
	void *columnRecord = malloc(200);
	while(rbfmsi.getNextRecord(crid,columnRecord) != 0){
		rbfm->deleteRecord(fileHandleC,columnDescriptor,crid);
	}
	free(value2);
	free(columnRecord);
	rbfmsi.close();
	rbfm->closeFile(fileHandleC);

	//delete the table by deleting the file
	rc = rbfm->destroyFile(tableName);
	if(rc!=0){
		return -1;
	}
    return 0;
}

/*int delete4Table(const vector<Attribute> &attrs, const void * data, int &tableID, const string &attribute){
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	RBFM_ScanIterator iterator;


	vector<Attribute> tableDescriptor;
	prepareAttribute4Table(tableDescriptor);

	rbfm -> openFile(TABLE, fileHandle);
	CompOp comparator = EQ_OP;
	string name = "table-name";
	//rbfm ->scan(fileHandle, tableDescriptor, attribute, comparator, data, attrs, iterator);
	//rbfm -> scan(fileHandle, tableDescriptor, name, comparator, data, attrs, scanner); //rbfm -> scan(fileHandle, tableDescriptor, attribute, comparator, data, attrs, scanner);
	return -1;

}*/


RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	RecordBasedFileManager *rfbm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	vector<Attribute> attributes;

	RC rc1 = getAttributes(tableName, attributes);
	if(rc1 < 0){
		return -1;
	}

	RC rc2 = rfbm -> openFile(tableName, fileHandle);

	if(rc2 < 0) {
		rfbm -> closeFile(fileHandle);
		return -1;
	}

	RC rc3 = rfbm -> readRecord(fileHandle, attributes, rid, data);
	if(rc3 < 0) {
		rfbm -> closeFile(fileHandle);
		return -1;
	}
	rfbm -> closeFile(fileHandle);
	return 0;

}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RC rc = rbfm -> printRecord(attrs, data);
	if(rc < 0){
		return -1;
	}

	return 0;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	vector<Attribute> attributes;
	RC rc1 = getAttributes(tableName, attributes);
	if(rc1 < 0) {
		return -1;
	}

	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	RC rc2 = rbfm -> openFile(tableName, fileHandle);
	if(rc2 < 0) {
		rbfm -> closeFile(fileHandle);
		return -1;
	}

	RC rc3 = rbfm -> readAttribute(fileHandle, attributes, rid, attributeName, data);

	if(rc3 < 0){
		rbfm -> closeFile(fileHandle);
		return -1;
	}

	rbfm -> closeFile(fileHandle);
    return 0;

}


RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    return -1;
}



// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}


// Assisting functions listed below:






