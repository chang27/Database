
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


}

RC RelationManager::deleteCatalog()
{
    return -1;
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


