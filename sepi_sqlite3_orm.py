from __future__ import annotations;

from typing import *;
import sqlite3;
import time;
#=============================
STRING_LITERAL_QUOTATION_MARK = '"';
SLQM = STRING_LITERAL_QUOTATION_MARK;
#=============================
class FieldType:
	def __init__(self, type:type, sqlite3_equivalent:str, args:Dict[str, Any], can_be_auto_incremented=True):
		self.type = type;
		self.sqlite3_equivalent = sqlite3_equivalent;
		self.args = args;
		self.can_be_auto_incremented = can_be_auto_incremented;

	def generate_sqlite_3_equivalent(self):
		return f"{self.sqlite3_equivalent}"


	def generate_default_value_literal(self, default_value:Any)->str:
		return str(default_value);


	def does_type_match(self, instance_of_that_type:Any)->bool:
		return isinstance(instance_of_that_type , self.type.__class__);


	def generate_literal(self, value:Any)->str:
		return str(value);



class StringLikeFiledType(FieldType):
	def __init__(self, type:type, sqlite3_equivalent:str, args:Dict[str, Any]):
		FieldType.__init__(self, type=type, sqlite3_equivalent=sqlite3_equivalent, args=args, can_be_auto_incremented=False);


	def generate_default_value_literal(self, default_value:Any)->str:
		return  f"{SLQM}{default_value}{SLQM}";


	def generate_literal(self, value)->str:
		return SLQM+value+SLQM;



class IntegerFieldType(FieldType):
	def __init__(self):
		FieldType.__init__(self, type=int, sqlite3_equivalent="INTEGER", args=dict());

	def does_type_match(self, instance_of_that_type:Any)->bool:
		output =  isinstance(instance_of_that_type , int);
		return output;
		


class TextFieldType(StringLikeFiledType):
	def __init__(self):
		StringLikeFiledType.__init__(self, type=str, sqlite3_equivalent="TEXT", args=dict());

	def does_type_match(self, instance_of_that_type:Any)->bool:
		return isinstance(instance_of_that_type , str);		


class FloatFieldType(FieldType):
	def __init__(self):
		FieldType.__init__(self, type=float, sqlite3_equivalent="REAL", args=dict());

	def does_type_match(self, instance_of_that_type:Any)->bool:
		return isinstance(instance_of_that_type , float);		

class BlobFieldType(StringLikeFiledType):
	def __init__(self):
		StringLikeFiledType.__init__(self, type=bytes, sqlite3_equivalent="BLOB", args=dict());

	def does_type_match(self, instance_of_that_type:Any)->bool:
		return isinstance(instance_of_that_type , bytes);		
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

		self.table_pointer = None;



	#====
	#error msgs generation 
	def emg__1(self):
		return f"field {self.name} from table {self.table_pointer.name}";

	#=====
	#checkers
	def is_default_value_defined(self):
		return self.default_value != None;


	def is_table_definition_pointed(self):
		return self.table_pointer != None and isinstance(table_pointer, TableDefintion);


	def can_be_left_un_valued(self):
		a = (self.cant_be_null == False);
		b = (self.is_default_value_defined() and self.cant_be_null==True);
		c = (self.has_auto_increment == True);
		return (a + b + c) > 0;


	def check_for_errors(self):
		if self.is_default_value_defined():
			if self.type.does_type_match(self.default_value) == False:
				raise Exception(self.emg__1() + f" default value's type {type(self.default_value)} does not matches the given type {self.type.type}");


	#=====
	#generators
	def generate_literal(self, value:Any)->str:
		return self.type.generate_literal(value=value);


	def generate_sqlite_3_equivalent(self)->str:
		output = f"{SLQM}{self.name}{SLQM}";
		output += " ";
		output += f"{self.type.generate_sqlite_3_equivalent()}"
		output += " ";
		if self.cant_be_null == True:
			output += "NOT NULL ";
		if self.default_value != None:
			output += "DEFAULT "
			output += self.generate_literal(value=self.default_value);
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
		self.fields_name_map = dict();

		for i1 in self.fields:
			self.fields_name_map[i1.name] = i1;

		for index in range(self.fields.__len__()):
			self.fields[index].table_pointer = self;

		for index in range(self.fields.__len__()):
			self.fields[index].check_for_errors();


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


	def return_field_by_name(self, name)->FieldDefinition:
		return self.fields_name_map[name];

	#======
	#creation command generator
	def return_end_of_creation_defintion_statement(self):
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


	def generate_creation_command(self)->str:
		self.check_and_raise_errors();
		output = str();
		output += f"CREATE TABLE {SLQM}{self.name}{SLQM}";
		output += "\n(";
		field_count = self.count_fields();
		for index, field in enumerate(self.fields):
			output += "\t"+field.generate_sqlite_3_equivalent();
			output += [",",","][field_count == index+1];
			output += "\n"

		output += "\t"+self.return_end_of_creation_defintion_statement();
		output += "\n";
		output += ")";
		output += ";";
		return output;

	#=========
	#select command generator
	def check_for_errors_of_given_values_for_insertion(self, given_values:Dict[str, Any])->None:
		for index, current_field in enumerate(self.fields):
			"""
			errors that can happen:
				+a field is missing while the field definition cant handle missing value (no allowed to be null or not having a default value)

				+given value has a mismatch with in type with field definition's type

			"""
			name_of_current_field = current_field.name;
			if name_of_current_field not in given_values:
				can_be_left_unvalued = current_field.can_be_left_un_valued();

				if can_be_left_unvalued:
					continue;

				elif can_be_left_unvalued== False:
					raise Exception(f"when inserting into table '{self.name}', a necessary filed '{name_of_current_field}' was missing from the given values");



			current_given_value = given_values[name_of_current_field];
			if current_field.type.does_type_match(current_given_value) == False:
				raise Exception(f"when inserting into table '{self.name}', a given value for field'{name_of_current_field}' has incompatible type; expected to be '{current_field.type.type} but was '{current_given_value.__class__}'");





	def generate__insertion_command(self, values:Dict[str, Any]):
		self.check_for_errors_of_given_values_for_insertion(given_values=values)
		output = f"INSERT INTO {self.name} ";
		requested_columns = "(";
		valueing_columns = "("

		number_of_given_values = len(values);

		for index, i1 in enumerate(values.items()):
			field_name, given_value  = i1;

			related_field = self.return_field_by_name(name=field_name);

			requested_columns += f"{field_name}";
			valueing_columns += related_field.generate_literal(given_value);

			if index+1 == number_of_given_values:
				pass

			elif index+1 != number_of_given_values:
				requested_columns += ",";
				valueing_columns += ",";

		requested_columns += ")";
		valueing_columns += ")"

		output = output + requested_columns + " VALUES " + valueing_columns;
		output += ";"
		return output;


	def check_given_field_names_and_desired_values(self, given_names_and_values:Dict[str, Any])->None:
		"""
		things that can go wrong:
			+a field which does not exist
			+a comparison value which is not same type as field definition
		"""
		for given_field_name, given_field_value in given_names_and_values.items():
			related_field = self.return_field_by_name(name=given_field_name)
			given_field_exists_in_definition_of_table = related_field != None
			if given_field_exists_in_definition_of_table == False:
				raise Exception(f"cant select rows of table '{self.name}' by field '{given_field_name}' which does not exist in table definition");

			if given_field_exists_in_definition_of_table == True:
				does_type_match = related_field.does_type_match(given_field_value);
				if does_type_match == False:
					raise Exception(f"type mismatch for field '{related_field.name}' from table '{self.name}' where type {related_field.type.__class__} was expected but {given_value.__class__} was given")



	def generate_selection_by_rigid_equlity_of_fields(self **fieldnames_and_desired_values)->str:
		self.check_given_field_names_and_desired_values(given_names_and_values=fieldnames_and_desired_values);
		output = f"SELECT * FROM {self.name} WHERE "



			
		

#==============================
class RowField:
	def __init__(self, field_name, value):
		self.field_name = field_name;
		self.value = value;


class QuerySelectOutput:
	def __init__(self):
		self.callback_id = None;
		self.output = None;
		self.execution_time = None;


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
				default_value=5,
				),	


			FieldDefinition(
				name="caption",
				type=TextFieldType(),
				cant_be_null=True,
				is_unique=False,
				has_auto_increment=False,
				is_primary_key=False,
				default_value="no caption :)",
				),				
			],
		);

	print(a.generate_creation_command());
	print(a.generate__insertion_command(values={"user_name":"asd", "password":"asd", "credit":1654}))


test_1();

