from __future__ import annotations;

from typing import *;
import sqlite3;
#=============================
STRING_LITERAL_QUOTATION_MARK = '"';
SLQM = STRING_LITERAL_QUOTATION_MARK;
#=============================
class FieldType:
	def __init__(self, type:type, sqlite3_equivalent:str, args:Dict[str, Any]):
		self.type = type;
		self.sqlite3_equivalent = sqlite3_equivalent;
		self.args = args;

	def generate_sqlite_3_equivalent(self):
		return f"{self.sqlite3_equivalent}"


	def generate_default_value_literal(self, default_value:Any)->str:
		return str(default_value);


class StringLikeFiledType(FieldType):
	def __init__(self, type:type, sqlite3_equivalent:str, args:Dict[str, Any]):
		FieldType.__init__(self, type=type, sqlite3_equivalent=sqlite3_equivalent, args=args);


	def generate_default_value_literal(self, default_value:Any)->str:
		return  f"{SLQM}{default_value}{SLQM}";



class IntegerFieldType(FieldType):
	def __init__(self):
		FieldType.__init__(self, type=int, sqlite3_equivalent="INTEGER", args=dict());


class TextFieldType(StringLikeFiledType):
	def __init__(self):
		StringLikeFiledType.__init__(self, type=str, sqlite3_equivalent="TEXT", args=dict());


class FloatFieldType(FieldType):
	def __init__(self):
		FieldType.__init__(self, type=float, sqlite3_equivalent="REAL", args=dict());


class BlobFieldType(StringLikeFiledType):
	def __init__(self):
		StringLikeFiledType.__init__(self, type=bytes, sqlite3_equivalent="BLOB", args=dict());

#=====


class FieldDefinition:
	"""
	types:
		int   -> INTEGER
		str   -> TEXT
		float -> REAL
		bytes -> BLOB

	"""
	def __init__(self, name:str, type:FieldType, cant_be_null:bool, is_unique:bool, has_auto_increment:bool, is_primary_key:bool, default_value:Any=None):
		self.name = name;
		self.type = type;
		self.is_unique = is_unique;
		self.is_primary_key = is_primary_key;
		self.default_value = default_value;
		self.cant_be_null = cant_be_null;
		self.has_auto_increment = has_auto_increment;

	#=====
	#checkers

	#=====
	#generators
	def generate_sqlite_3_equivalent(self)->str:
		output = f"{SLQM}{self.name}{SLQM}";
		output += " ";
		output += f"{self.type.generate_sqlite_3_equivalent()}"
		output += " ";
		if self.cant_be_null == True:
			output += "NOT NULL ";
		if self.default_value != None:
			output += "DEFAULT "
			output += self.type.generate_default_value_literal(default_value=self.default_value);
			output += " ";
		if self.is_unique == True:
			output += " ";
			output += "UNIQUE";
		output += "";
		return output;








class TableDefintion:
	def __init__(self, name, fields:List[FieldDefinition]):
		self.name = name;
		self.fields = fields;

	#counters
	def count_fields(self):
		return self.fields.__len__();


	def count_number_of_field_which_are_primary(self):
		count = 0;
		for i1 in self.fields:
			count += i1.is_primary_key == True;
		return count;


	def count_number_of_auto_increment_fields(self):
		count = 0;
		for i1 in self.fields:
			count += i1.has_auto_increment == True;
		return count;

	#=======
	#checkers 

	def has_only_1_field_which_is_ai_and_pk(self):
		count = 0;
		for i1 in self.fields:
			if i1.has_auto_increment and i1.is_primary_key:
				count += 1;
		return count == 1;


	def has_multiple_fields_which_are_pk_and_no_ai(self):
		return self.count_number_of_field_which_are_primary() > 1 and self.count_number_of_auto_increment_fields() == 0 ;


	def has_only_1_field_which_is_pk_and_not_ai(self):
		return self.count_number_of_field_which_are_primary() == 1 and self.count_number_of_auto_increment_fields() == 0;


	def has_at_least_one_field_which_is_pk(self):
		for i1 in self.fields:
			if i1.is_primary_key == True:
				return True;
		return False;
	#error checkers =======
	def error_checker__having_more_than_pk_and_a_ai(self):
		return self.count_number_of_field_which_are_primary() > 1 and self.count_number_of_auto_increment_fields() == 1;


	def check_and_raise_errors(self):
		if self.error_checker__having_more_than_pk_and_a_ai():
			raise Exception(f"definition of table '{self.name}' has more than 1 primary key and one auto increment; only one auto increment is allowed")
	#=======
	def return_nth_filed(self, index:int)->FieldDefinition:
		return self.fields[index];

	def return_first_primary_key_field(self)->FieldDefinition:
		for i1 in self.fields:
			if i1.is_primary_key == True:
				return i1;

	def return_all_primary_key_fields(self)->List[FieldDefinition]:
		output = list();
		for i1 in self.fields:
			if i1.is_primary_key == True:
				output.append(i1);
		return output;

	#======
	def return_end_of_defintion_statement(self):
		output = str();
		if self.has_at_least_one_field_which_is_pk() == True:
			output += "PRIMARY KEY ("
			if self.has_only_1_field_which_is_pk_and_not_ai() == True:
				output += f"'{self.return_first_primary_key_field().name}'";


			elif self.has_only_1_field_which_is_ai_and_pk() == True:
				output += f"'{self.return_first_primary_key_field().name}' AUTOINCREMENT";


			elif self.has_multiple_fields_which_are_pk_and_no_ai() == True:
				for i1 in return_all_primary_key_fields():
					output += f"'{i1.name}'";


			output += ")"


		return output;


	def generate_command(self)->str:
		self.check_and_raise_errors();
		output = str();
		output += f"CREATE TABLE {SLQM}{self.name}{SLQM}";
		output += "\n(";
		field_count = self.count_fields();
		for index, field in enumerate(self.fields):
			output += "\t"+field.generate_sqlite_3_equivalent();
			output += [",",","][field_count == index+1];
			output += "\n"

		output += "\t"+self.return_end_of_defintion_statement();
		output += "\n";
		output += ")";
		return output;
		

#==============================
class RowField:
	def __init__(self, field_name, value):
		self.field_name = field_name;
		self.value = value;


class QuerySelectOutput:
	def __init__(self, callback_id:str, output:List[TableRow], execution_time:float):
		self.callback_id = callback_id;
		self.output = output;
		self.execution_time = execution_time;


class TableRow:
	def __init__(self, model_belonging_to:ModelDefintion, fields:List[RowField]):
		self.model_belonging_to = model_belonging_to;
		self.fields = fields;

#==============================
class Sepi_SQLite3_ORM:
	def __init__(self, file_name:str,):
		self.file_name = file_name;
		self.connection = None;

	def is_connection_made(self)->bool:
		return self.connection == None;


	def make_connection(self)->bool:
		self.connection = sqlite3.connect(self.file_name);


	def execute_commit_fetch(self, command):
		print("execute_and_return_output");
		print("commmand:");
		print(f"{command}");
		print("-------------");
		print("forming the cursor");
		cursor = self.connection.cursor();
		print("trying to execute the command");
		cursor.execute(command);
		print("executed the command");
		self.connection.commit();
		print("commited")
		print("getting the output");
		output = cursor.fetchall();
		print("output:")
		print(output.__str__().replace("),", "),\n"));
		return output;





#===================TESTS


def test_1():
	a = TableDefintion(
		name="user",
		fields=[

			FieldDefinition(
				name="uuid",
				type=IntegerFieldType(),
				cant_be_null=True,
				is_unique=True,
				has_auto_increment=True,
				is_primary_key=True,
				),


			FieldDefinition(
				name="user_name",
				type=TextFieldType(),
				cant_be_null=True,
				is_unique=True,
				has_auto_increment=False,
				is_primary_key=False,
				),


			FieldDefinition(
				name="password",
				type=TextFieldType(),
				cant_be_null=True,
				is_unique=False,
				has_auto_increment=False,
				is_primary_key=False,
				),	


			FieldDefinition(
				name="credit",
				type=IntegerFieldType(),
				cant_be_null=True,
				is_unique=False,
				has_auto_increment=False,
				is_primary_key=False,
				default_value=0,
				),	
			],
		);

	print(a.generate_command());


test_1();

