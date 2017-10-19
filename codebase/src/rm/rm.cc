
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
	buffer = malloc(100);
	vector<Attribute> columnDescriptor;
	prepareAttribute4Column(columnDescriptor);



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

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    return -1;
}

RC RelationManager::deleteTable(const string &tableName)
{
    return -1;
}

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
    return -1;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    return -1;
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

void prepareAttribute4Colum(vector<Attribute> &columnDescriptor) {
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

	columnDescriptor = {};
	columnDescriptor.push_back(table_id);
	columnDescriptor.push_back(column_name);
	columnDescriptor.push_back(column_type);
	columnDescriptor.push_back(column_length);
	columnDescriptor.push_back(column_position);

}

void prepareRecord4Tables(const int tid, const char *tableName, const int tlen, const char *fileName,
		const int flen, int pointerSize, void *data) {
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

}




