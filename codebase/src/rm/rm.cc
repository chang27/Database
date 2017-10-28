
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
/********test use only**************/
void printAttribute(vector<Attribute> tableAttribute) {
	for (size_t i = 0; i < tableAttribute.size(); i++) {
		cout << "Attribute " << i << ": " << tableAttribute[i].name << " type: " << tableAttribute[i].type << " length: "<< tableAttribute[i].length << endl;
	}
}
/************test ends*************/

/****not changed for version***/
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
	Attribute table_id, table_name, file_name, table_version;
	table_id.name = "table-id";
	table_id.type = TypeInt;
	table_id.length = sizeof(int);

	table_name.name = "table-name";
	table_name.type = TypeVarChar;
	table_name.length = 50;

	file_name.name = "file-name";
	file_name.type = TypeVarChar;
	file_name.length = 50;

	table_version.name = "version";
	table_version.type = TypeInt;
	table_version.length = sizeof(int);

	//tableDescriptor = {};
	tableDescriptor.push_back(table_id);
	tableDescriptor.push_back(table_name);
	tableDescriptor.push_back(file_name);
	tableDescriptor.push_back(table_version);
}

void prepareAttribute4Column(vector<Attribute> &columnDescriptor) {
	Attribute table_id, column_name, column_type, column_length, column_position, table_version;

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

	table_version.name = "version";
	table_version.type = TypeInt;
	table_version.length = sizeof(int);
	//columnDescriptor = {};
	columnDescriptor.push_back(table_id);
	columnDescriptor.push_back(column_name);
	columnDescriptor.push_back(column_type);
	columnDescriptor.push_back(column_length);
	columnDescriptor.push_back(column_position);
	columnDescriptor.push_back(table_version);

}

void prepareRecord4Tables(const int tid, const char *tableName, int tlen, const char *fileName,
		 int flen, const int pointerSize, void *data, const int version) {
	unsigned char *nullPointer = (unsigned char *)malloc(pointerSize);
	memset(nullPointer, 0, pointerSize);
	int start = 0;
	memcpy((char *)data + start, nullPointer, pointerSize);
	start += pointerSize;
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
	memcpy((char *)data +start, &version, 4);
	start += 4;
	free(nullPointer);

}
//prepareRecord4Columns(tid, attri.name.c_str(), attri.name.size(), attri.type, attri.length, i, nullPointerSize, buffer);
void prepareRecord4Columns(const int cid, const char* columnName, const int clen, AttrType type, const int len, const int pos, const int pointerSize, void * data,const int version) {
	unsigned char* nullPointer = (unsigned char *)malloc(pointerSize);
	memset(nullPointer, 0, pointerSize);
	int start = 0;
	memcpy((char *)data + start, nullPointer, pointerSize);
	start += pointerSize;
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
	memcpy((char *)data + start, &version, 4);
	free(nullPointer);
}
// no change
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
	memcpy(value,&nameSize,4);
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

int callDeleteRecord(const string &fileName, const vector<Attribute>& descriptor,const RID &rid) {
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
	int version = 1;
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
	void *buffer = malloc(1000);
	vector<Attribute> tableDescriptor;
	prepareAttribute4Table(tableDescriptor);
	int pointerSize = ceil((double) tableDescriptor.size() / 8);
	prepareRecord4Tables(tid, TABLE, 6 , TABLE, 6, pointerSize, buffer,version);

	RID rid1;
	rbfm->insertRecord(tableHandle, tableDescriptor, buffer, rid1);
	free(buffer);



	void *buffer2 = malloc(1000);
	//int pointerSize4C = ceil((double) columnDescriptor.size() / 8);

	prepareRecord4Tables(cid, COLUMN, 7, COLUMN, 7, pointerSize, buffer2,version);
	RID rid2;
	rbfm -> insertRecord(tableHandle, tableDescriptor, buffer2, rid2);
	free(buffer2);

	rbfm -> closeFile(tableHandle);


	// create Columns:
	vector<Attribute> columnDescriptor;
	prepareAttribute4Column(columnDescriptor);

    //cout << columnDescriptor.size() << endl;

	FileHandle columnHandle;
	rbfm -> openFile(COLUMN, columnHandle);

	int nullPointerSize = ceil((double) columnDescriptor.size() / 8);

	for(int i = 1; i <= tableDescriptor.size(); i++){
		Attribute attri = tableDescriptor[i-1];
		void *buffer = malloc(1000);
		prepareRecord4Columns(tid, attri.name.c_str(), attri.name.size(), attri.type, attri.length, i, nullPointerSize, buffer,version);
		RID rid;
		rbfm -> insertRecord(columnHandle, columnDescriptor, buffer, rid);
		free(buffer);
	}

	for(int i = 1; i <= columnDescriptor.size(); i++) {
		Attribute attri2 = columnDescriptor[i-1];
		void *buffer = malloc(1000);
		prepareRecord4Columns(cid, attri2.name.c_str(), attri2.name.size(), attri2.type, attri2.length, i, nullPointerSize, buffer,version);
		RID rid;
		rbfm -> insertRecord(columnHandle, columnDescriptor, buffer, rid);
		free(buffer);
	}

	rbfm -> closeFile(columnHandle);
	return 0;
}
// no change
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

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	int version = 1;
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


	//insert into TABLE:
	vector<Attribute> tableDescriptor;
	prepareAttribute4Table(tableDescriptor);
	int pointerSize = ceil((double) tableDescriptor.size() / 8);

	void *buffer = malloc(1000);
	prepareRecord4Tables(newID, tableName.c_str(), tableName.size(), tableName.c_str(), tableName.size(), pointerSize, buffer,version);
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
		void *buffer = malloc(1000);
		prepareRecord4Columns(newID, attribute.name.c_str(), attribute.name.size(), attribute.type, attribute.length, i, columnPointerSize, buffer,version);
		RID rid_i;
		RC rci = callInsertRecord(COLUMN, columnDescriptor, buffer, rid_i);
		free(buffer);
		if(rci < 0) return rci;
	}
    return 0;
}
// no change
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

	int nameSize = tableName.length();
	memcpy(value1, &nameSize, 4);
	memcpy((char *)value1 + 4, tableName.c_str(), nameSize);


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
	void *tableRecord = malloc(1600);
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
	rbfmsi.close();
	int tableID = *(int *)((char *)tableRecord+1);
	free(value1);
	free(tableRecord);

	rbfm->closeFile(fileHandleT);

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
	while(rbfmsi.getNextRecord(crid,columnRecord) != RBFM_EOF){
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

Attribute fromAttribute(void *data, int &currentVersion){
	Attribute result;
	int nameSize = *(int *)((char *)data+1);
	void *attrName= malloc(nameSize);
	memcpy(attrName,(char *)data+5,nameSize);
	string attrNameS((char *)attrName,nameSize);
	result.name = attrNameS;

	int type = *(int *)((char *)data+5+nameSize);
	if(type==0){
		result.type = TypeInt;
	}else if(type==1){
		result.type = TypeReal;
 	}else{
 		result.type = TypeVarChar;
 	}

	int attrLength = *(int *)((char *)data +5+nameSize+4);
	result.length = attrLength;

	currentVersion = *(int *)((char *)data +5+nameSize + 4+4);
	return result;
}
/*int getLatestVersion(const string &tableName, int &tableID){
	int result=0;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RBFM_ScanIterator rbfmsi=RBFM_ScanIterator();
	FileHandle fileHandleT;
	rbfm->openFile(TABLE,fileHandleT);
	//cout<<"the TABLE has been opened"<<endl;
	vector<Attribute> tableDescriptor;
	prepareAttribute4Table(tableDescriptor);
	string conditionAttribute1 = "table-name";
	void *value1 = malloc(54);
	int nameSize = tableName.size();
	memcpy(value1, &nameSize, 4);
	memcpy((char *)value1+4,tableName.c_str(),nameSize);
	vector<string> attributeNames1;
	attributeNames1.push_back("table-id");
	attributeNames1.push_back("version");

	RC rc=rbfm->scan(fileHandleT,tableDescriptor,conditionAttribute1,EQ_OP,value1,attributeNames1,rbfmsi);
//	cout<<"the rc for scan is "<<rc<<endl;
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
	//cout<<"the rc for getNextRecord is "<<rc<<endl;
	if(rc != 0){
		free(value1);
		free(tableRecord);
		rbfmsi.close();
		rbfm->closeFile(fileHandleT);
		return -1;
	}

	tableID = *(int *)((char *)tableRecord+1);

	result = *(int *)((char *)tableRecord +5);

	//cout<<"the tableID is "<<tableID<<endl;
	free(value1);
	free(tableRecord);
	rbfm->closeFile(fileHandleT);
	rbfmsi.close();
	return result;
}*/

RC getLatestVersion(const string &tableName, const bool &update, vector<int> & res){
	//vector<int> res;
	cout<<"check out 527"<<endl;
	//res[1]=0;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RBFM_ScanIterator rbfmsi=RBFM_ScanIterator();
	FileHandle fileHandleT;
	RC rc9 = rbfm->openFile(TABLE,fileHandleT);
	cout<<"the TABLE open file "<<rc9<<endl;
	vector<Attribute> tableDescriptor;
	prepareAttribute4Table(tableDescriptor);
	string conditionAttribute1 = "table-name";
	void *value1 = malloc(54);
	int nameSize = tableName.size();
	memcpy(value1, &nameSize, 4);
	memcpy((char *)value1+4,tableName.c_str(),nameSize);
	vector<string> attributeNames1;
	attributeNames1.push_back("table-id");
	attributeNames1.push_back("version");

	RC rc=rbfm->scan(fileHandleT,tableDescriptor,conditionAttribute1,EQ_OP,value1,attributeNames1,rbfmsi);
	cout<<"the rc for scan is "<<rc<<endl;
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
	cout<<"the rc for getNextRecord is "<<rc<<endl;
	if(rc != 0){
		free(value1);
		free(tableRecord);
		rbfmsi.close();
		rbfm->closeFile(fileHandleT);
		return -1;
	}

	res.push_back(*(int *)((char *)tableRecord+1));//0 is tableID

	res.push_back(*(int *)((char *)tableRecord +5));//1 is the latestVersion

	if(res[1] == 0){
		return -1;
	}
	cout<<"the table id is "<<res[0]<<" the latest version is "<<endl;
	if(update){
		int newVersion = res[1]+1;
		void *oldRecord = malloc(1000);//actually no more than 118
		int pointerSize = ceil((double) tableDescriptor.size() / 8);
		//rbfm->readRecord(fileHandleT,tableDescriptor,trid,oldRecord);
		prepareRecord4Tables(res[0], tableName.c_str(), tableName.size(), tableName.c_str(),
				 tableName.size(), pointerSize, oldRecord, newVersion);
//		RC rc3= rbfm->deleteRecord(fileHandleT,tableDescriptor,trid);
//		RID newRid;
//		RC rc4 = callInsertRecord(TABLE, tableDescriptor, oldRecord, newRid);
//		free(oldRecord);
//		if(rc3==-1 || rc4==-1) return -1;
		RC rc2=rbfm->updateRecord(fileHandleT,tableDescriptor,oldRecord,trid);
		free(oldRecord);
		if(rc2<0) return -1;
	}

	//cout<<"the tableID is "<<tableID<<endl;
	free(value1);
	free(tableRecord);
	rbfm->closeFile(fileHandleT);
	rbfmsi.close();
	return 0;
}
RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	//This method gets the attributes of latest version
	//given the tableName, scan TABLE and find the table-id

	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RBFM_ScanIterator rbfmsi=RBFM_ScanIterator();
	FileHandle fileHandleT;
	rbfm->openFile(TABLE,fileHandleT);
	//cout<<"the TABLE has been opened"<<endl;
	vector<Attribute> tableDescriptor;
	prepareAttribute4Table(tableDescriptor);
	string conditionAttribute1 = "table-name";
	void *value1 = malloc(54);
	int nameSize = tableName.size();
	memcpy(value1, &nameSize, 4);
	memcpy((char *)value1+4,tableName.c_str(),nameSize);
	vector<string> attributeNames1;
	attributeNames1.push_back("table-id");
	attributeNames1.push_back("version");

	RC rc=rbfm->scan(fileHandleT,tableDescriptor,conditionAttribute1,EQ_OP,value1,attributeNames1,rbfmsi);
//	cout<<"the rc for scan is "<<rc<<endl;
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
	//cout<<"the rc for getNextRecord is "<<rc<<endl;
	if(rc != 0){
		free(value1);
		free(tableRecord);
		rbfmsi.close();
		rbfm->closeFile(fileHandleT);
		return -1;
	}

	int tableID = *(int *)((char *)tableRecord+1);

	int latestVersion = *(int *)((char *)tableRecord +5);


	//cout<<"the tableID is "<<tableID<<endl;
	free(value1);
	free(tableRecord);
	rbfm->closeFile(fileHandleT);
	rbfmsi.close();

	//given the table-id, scan the COLUMN, find all the attributes and push back

	//get the parameters ready for scan the COLUMN
	FileHandle fileHandleC;
	rbfm->openFile(COLUMN,fileHandleC);
	vector<Attribute> columnDescriptor;
	prepareAttribute4Column(columnDescriptor);
	string conditionAttribute2 = "table-id";
	void *value2 = malloc(4);
	memcpy(value2,&tableID,4);
	vector<string> attributeNames2;
	//attributeNames2.push_back("table-id");
	attributeNames2.push_back("column-name");
	attributeNames2.push_back("column-type");
	attributeNames2.push_back("column-length");
	attributeNames2.push_back("version");


	//scan COLUMN by table-id, get the colun-name of the table
	rc= rbfm->scan(fileHandleC,columnDescriptor,conditionAttribute2,EQ_OP,value2,attributeNames2,rbfmsi);
	if(rc!=0){
		free(value2);
		rbfmsi.close();
		rbfm->closeFile(fileHandleC);
		return rc;
	}
	//push back the column names into attris
	RID crid;
	void *columnRecord = malloc(200);
	Attribute attr;
	string columnNameStr;
	int currentVersion = 0;
	while(rbfmsi.getNextRecord(crid,columnRecord) != -1){
			attr = fromAttribute(columnRecord,currentVersion);
			if(currentVersion == latestVersion){
			attrs.push_back(attr);
			}
	}
	free(value2);
	free(columnRecord);
	rbfmsi.close();
	rbfm->closeFile(fileHandleC);
    return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	if(tableName == TABLE ||tableName == COLUMN) return -1;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	RC rc = rbfm->openFile(tableName,fileHandle);
	if(rc==-1) return rc;

	vector<Attribute> recordDescriptor;
	rc = getAttributes(tableName,recordDescriptor);
	if(rc==-1) return rc;

	rc = rbfm->insertRecord(fileHandle,recordDescriptor,data,rid);
	if(rc==-1){
		rbfm->closeFile(fileHandle);
		return rc;
	}
	rbfm->closeFile(fileHandle);
    return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	if(tableName == TABLE ||tableName == COLUMN) return -1;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	RC rc = rbfm->openFile(tableName,fileHandle);
	if(rc==-1) return rc;

	vector<Attribute> recordDescriptor;
	rc = getAttributes(tableName,recordDescriptor);
	if(rc==-1) return rc;

	rc = rbfm->deleteRecord(fileHandle,recordDescriptor,rid);
		if(rc==-1){
			rbfm->closeFile(fileHandle);
			return rc;
		}
	rbfm->closeFile(fileHandle);
	return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	if(tableName == TABLE ||tableName == COLUMN) return -1;
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	RC rc = rbfm->openFile(tableName,fileHandle);
	if(rc==-1) return rc;

	vector<Attribute> recordDescriptor;
	rc = getAttributes(tableName,recordDescriptor);
	if(rc==-1) return rc;

	rc = rbfm->updateRecord(fileHandle,recordDescriptor,data,rid);
	if(rc==-1){
		rbfm->closeFile(fileHandle);
		return rc;
	}
	rbfm->closeFile(fileHandle);
    return 0;
}

RC getOldAttributes(const string &tableName, const int &oldVersion, const int &tableID, vector<Attribute> &oldAttributes){
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	RBFM_ScanIterator rbfmsi=RBFM_ScanIterator();
	FileHandle fileHandle;
	rbfm->openFile(COLUMN,fileHandle);
	vector<Attribute> columnDescriptor;
	prepareAttribute4Column(columnDescriptor);
	string conditionAttribute = "table-id";
	void *value = malloc(4);
	memcpy(value,&tableID,4);
	vector<string> attributeNames;
	attributeNames.push_back("column-name");
	attributeNames.push_back("column-type");
	attributeNames.push_back("column-length");
	attributeNames.push_back("version");


	//scan COLUMN by table-id, get the colun-name of the table
	RC rc=rbfm->scan(fileHandle,columnDescriptor,conditionAttribute,EQ_OP,value,attributeNames,rbfmsi);
	if(rc!=0){
		free(value);
		rbfmsi.close();
		rbfm->closeFile(fileHandle);
		return rc;
		}
	//push back the column names into attris
	RID crid;
	void *columnRecord = malloc(200);
	Attribute attr;
	string columnNameStr;
	int currentVersion = 0;
		while(rbfmsi.getNextRecord(crid,columnRecord) != -1){
			attr = fromAttribute(columnRecord,currentVersion);
				if(currentVersion == oldVersion){
				oldAttributes.push_back(attr);
				}
		}
	free(value);
	free(columnRecord);
	rbfmsi.close();
	rbfm->closeFile(fileHandle);
	return 0;
}
RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	vector<Attribute> latestAttributes;
	RC rc1 = getAttributes(tableName, latestAttributes);// this is the latest attributes
	if(rc1 < 0){
		return -1;
	}
	RC rc2 = rbfm -> openFile(tableName, fileHandle);
	if(rc2 < 0) {
		rbfm -> closeFile(fileHandle);
		return -1;
	}
	//int tableID = 0;
	int currentVersion = rbfm->getRecordVersion(fileHandle, rid);
	bool update = false;

	vector<int> res;
	RC rc5	= getLatestVersion(tableName,update, res);
	if(rc5<0) return -1;
	int latestVersion = res[1];
	int tableID = res[0];
	//int latestVersion = getLatestVersion(tableName,tableID,update);
	//int latestVersion = getLatestVersion(tableName,tableID);
	if(currentVersion == -1) return -1;
	cout<<"the current version is "<<currentVersion <<"the latest version is "<<latestVersion<<endl;
	//this is the current version, attributes are right
	if(currentVersion == latestVersion){
		rc2 = rbfm -> readRecord(fileHandle, latestAttributes, rid, data);
		if(rc2 < 0) {
			rbfm -> closeFile(fileHandle);
			return -1;
		}
	}else{//this is an old version, we need to compare and output the latest version
		vector<Attribute> currentAttributes;
		RC rc3 = getOldAttributes(tableName, currentVersion, tableID, currentAttributes);
		if(rc3<0) return -1;

		void *buffer = malloc(PAGE_SIZE);
		rbfm->getOriginalRecord(fileHandle,rid, buffer); //get the original record with field offset
		int m = currentAttributes.size();
		int n = latestAttributes.size();
		int pointerSize = ceil(n/8);
		//int oldPointerSize = ceil(m/8);
		int offset = pointerSize;

		int nullArray[n];

		for(int i=0; i<n; i++){
			nullArray[i]= 1;
			int j=0;
			for(;j<m;j++){
				// the attribute that exist should memcpy to data
				if(currentAttributes[j].name == latestAttributes[i].name){
					nullArray[i]=0;//update the null pointer
					short fieldStart = *(short *)((char *)buffer+2*j);
					short fieldEnd = *(short *)((char *)buffer+2*(j+1));

					memcpy((char *)data+offset, (char *)buffer+2*(m+1)+fieldStart, fieldEnd-fieldStart);//update the date
					offset += fieldEnd-fieldStart;
					break;
				}
			}
			if(j==m){//we didn't find the field in the current attributes
				nullArray[i]=1;
				//no update about data
			}
		}
		unsigned char *nullPointer = (unsigned char *) malloc(pointerSize);

		for(int i = 0; i < pointerSize; i++) {
		            int sum = 0;
		            int j = 8*i;
		            while(j < (i+1)*8 && j < n){
		                sum |= nullArray[j]*(1 << (8 -1 - j%8));
		                j++;
		            }
		            *(nullPointer + i) = sum;
		        }
		memcpy((char *)data, nullPointer, pointerSize);
	}

	rbfm -> closeFile(fileHandle);
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

	RBFM_ScanIterator rbfmsi = RBFM_ScanIterator();

	vector<Attribute> recordDescriptor;
	RC rc = getAttributes(tableName,recordDescriptor);
	if(rc < 0 ) return rc;

	RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
	FileHandle fileHandle;
	rbfm->openFile(tableName,fileHandle);
	rc = rbfm->scan(fileHandle,recordDescriptor,conditionAttribute,compOp,value,attributeNames,rbfmsi);
	if(rc != 0){
		//rbfmsi.close();
		rbfm->closeFile(fileHandle);
		return -1;
	}
	rm_ScanIterator.setRBFMSI(rbfmsi);
    return 0;
}
RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {

	RC rc = rbfmsi.getNextRecord(rid, data);
	if(rc== RBFM_EOF){
		return RM_EOF;
	}
	return 0;
}


// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{

	//check if table exist and table is not system one:
		if(! tableNameOccuppied(tableName)){
			return -1;
		}
		if(tableName == TABLE || tableName == COLUMN) {
			return -1;
		}

		int index = -1;
		vector<Attribute> attributes;
		RC rc = getAttributes(tableName, attributes);//this is the latest attribtues
		if(rc < 0) return -1;

		cout<<"check out 983"<<endl;
		for(int i = 0; i < attributes.size(); i++) {
			if(attributes[i].name == attributeName){
				index = i;
				break;
			}
		}
		cout<<"check out 990"<<endl;
		cout<<"index is "<<index<<endl;
		if(index == -1) return -1; // attribute not found;

		RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();
		//int tableID;
		bool update=true; //also update the version in TABLE
		//int latestVersion = getLatestVersion(tableName, tableID, update);
		//vector<int> res = getLatestVersion(tableName,update);
		vector<int> res;
		cout<<"check out 1000"<<endl;
		RC rc5	= getLatestVersion(tableName,update, res);
		cout<<"get lastest version "<< rc5<<endl;
		if(rc5<0) return -1;
		int tableID = res[0];
		int latestVersion = res[1];
		//int latestVersion = getLatestVersion(tableName,tableID);
		cout<<"check out 927"<<endl;
		int curVersion = latestVersion + 1;

		vector<Attribute> columnDescriptor;
		prepareAttribute4Column(columnDescriptor);
		FileHandle columnHandle;
		rbfm -> openFile(COLUMN, columnHandle);
		int nullPointerSize = ceil((double) columnDescriptor.size() / 8);
		void *buffer;

		for(int i = 0; i < attributes.size(); i++) {
			Attribute attri = attributes[i];
			if(i == index) continue;
			buffer = malloc(100);
			if(i < index){
				// here need add version number attribute!!!
				prepareRecord4Columns(tableID, attri.name.c_str(), attri.name.size(), attri.type, attri.length, i + 1, nullPointerSize, buffer,curVersion);
			}else if( i > index){
				prepareRecord4Columns(tableID, attri.name.c_str(), attri.name.size(), attri.type, attri.length, i, nullPointerSize, buffer,curVersion);
			}

			RID rid;
			rbfm -> insertRecord(columnHandle, columnDescriptor, buffer, rid);
			free(buffer);

		}

    return 0;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{

	// check if tableName already exist in TABLE;
	if(tableNameOccuppied(tableName)){
		return -1;
	}
	vector<Attribute> attrs;
	getAttributes(tableName,attrs);//this is the latest attributes

	for(int i=0;i<attrs.size();i++){
		if(attrs[i].name == attr.name) return-1;
	}
	//int tableID;
	bool update = true;
	//int latestVersion = getLatestVersion(tableName, tableID,update);
	//int latestVersion = getLatestVersion(tableName,tableID);
	//vector<int> res = getLatestVersion(tableName,update);
	vector<int> res;
	RC rc5	= getLatestVersion(tableName,update, res);
	if(rc5<0) return -1;
	int tableID = res[0];
	int latestVersion = res[1];
	//update the TABLE version

	//insert into COLUMN:
	vector<Attribute> columnDescriptor;
	prepareAttribute4Column(columnDescriptor);
	int columnPointerSize = ceil((double) columnDescriptor.size() / 8);
	int i=1;
	for(; i <= attrs.size(); i++) {
		Attribute attribute = attrs[i-1];
		void *buffer = malloc(1000);
		prepareRecord4Columns(tableID, attribute.name.c_str(), attribute.name.size(), attribute.type, attribute.length, i, columnPointerSize, buffer,latestVersion+1);
		RID rid_i;
		RC rci = callInsertRecord(COLUMN, columnDescriptor, buffer, rid_i);
		free(buffer);
		if(rci < 0) return rci;
		}

	void *buffer = malloc(1000);
	prepareRecord4Columns(tableID,attr.name.c_str(),attr.name.size(),attr.type,attr.length,i+1, columnPointerSize, buffer, latestVersion+1);
	RID rid_i;
	RC rci = callInsertRecord(COLUMN, columnDescriptor, buffer, rid_i);
	free(buffer);
	if(rci < 0) return rci;
    return 0;
}


// Assisting functions listed below:






