import sqlparse;
import os;

def process_line(line):
	line=line.lstrip();
	line=line.replace('\r','');
	line=line.replace('\n','');
	return line;

def filter_tokens(tokens):
	proper_tokens=[];

	for i in tokens:
		if(i.is_whitespace() != True):
			proper_tokens.append(i);
	return proper_tokens;

def create_query(tokens,database):
	tokens=filter_tokens(tokens);
	function_token=filter_tokens(tokens[-1].tokens);
	table_name=function_token[0].value;
	inside_punctuation = filter_tokens(function_token[1].tokens)[1];
	try:
		inside_punctuation=inside_punctuation.get_identifiers();
	except:
		inside_punctuation=[inside_punctuation];
	attr=[];
	attr_type=[];
	for i in inside_punctuation:
		attr_tokens=filter_tokens(i.tokens);
		attr.append(attr_tokens[0].value);
		attr_type.append(attr_tokens[1].value);
	if len(attr) == len(set(attr)):
		attr=[table_name] + attr;
		database.create_empty_table(attr);
	
	else:
		return False;

def insert_query(tokens,database):
	tokens=filter_tokens(tokens);
	table_name=tokens[2].value;	
	function_token=filter_tokens(tokens[-1].tokens);
	inside_punctuation =function_token[1];
	try:
		inside_punctuation=inside_punctuation.get_identifiers();
	except:
		inside_punctuation=[inside_punctuation];

	values=[];
	for i in inside_punctuation:
		values.append(i.value);
	table_obj=None;
	for i in database.table_list:
		if(i.name==table_name):
			table_obj=i;
			break;
	if(table_obj==None):
		return False;
	table_obj.insert_record(values,1);
	return True;





def truncate_query(tokens,database):
	tokens=filter_tokens(tokens);
	table_name=tokens[-1].value;
	#print table_name;
	database.truncate_table(table_name);


def drop_query(tokens,database):
	tokens=filter_tokens(tokens);
	table_name=tokens[-1].value;
	f=open(table_name+'.csv');
	for line in f:
		line=process_line(line);
		if(line!=''):
			print "Not Empty"
			return False;
	#print table_name;
	database.delete_table(table_name);




def filter_conditions(table,condition):
	if condition==None:
		return;

def compare(a,b,comparator):

	if(comparator=="="):
		return a==b;

	if(comparator==">="):
		return a>=b;

	if(comparator=="<="):
		return a<=b;

	if(comparator==">"):
		return a>b;
	if(comparator=="<"):
		return a<b;


def logic(a,b,comp):
	if(comp.upper()=='AND'):
		return a and b;
	if(comp.upper()=='OR'):
		return a or b;

def cond_verify(line,attr,statement):

	if(statement==None):
		return True;

	tk=filter_tokens(statement.tokens);
	tk=tk[1:]
	truth=[];
	comparision=[];
	semi_attr=[i.split('.')[1] for i in attr];
	for i in tk:
		if(isinstance(i,sqlparse.sql.Comparison)):
			st_tokens=filter_tokens(i.tokens);
			attr1=st_tokens[0];
			attr2=st_tokens[2];
			comp=st_tokens[1];

			if(isinstance(attr1,sqlparse.sql.Identifier)):
				if('.' not in attr1.value and semi_attr.count(attr1.value)==1):
					ik=semi_attr.index(attr1.value);
					attr1.value=attr[ik];
				elif('.' not in attr1.value):
					return False;
				attr1=int(line[attr.index(attr1.value)]);
			else:
				attr1=int(attr1.value);


			if(isinstance(attr2,sqlparse.sql.Identifier)):
				if('.' not in attr2.value and semi_attr.count(attr2.value)==1):
					ik=semi_attr.index(attr2.value);
					attr2.value=attr[ik];

				elif('.' not in attr2.value):
					return False;
				attr2=int(line[attr.index(attr2.value)]);
			else:
				attr2=int(attr2.value);

			truth.append(compare(attr1,attr2,comp.value));
		else:
			comparision.append(i.value);

	if(len(comparision)==1):
		return logic(truth[0],truth[1],comparision[0]);
	else:
		return  truth[0];




def select_rows(table_name,atb,dis_attr,st_attr,attr,cond_statement):

	f=open(table_name,'r');
	rem={};
	#print atb;
	#print dis_attr;
	#print st_attr
	#print attr;
	#print cond_statement

	if(None not in st_attr):
		print st_attr;
		return;

	for line in f:
		line=process_line(line);
		if(line==''):
			continue;
		line=line.split(',');
		i=0;
		st='';
		co=0;
		if(cond_verify(line,attr,cond_statement)==False):
			continue;
		for i in atb:
			if(isinstance(i,unicode) or isinstance(i,str)):
				ind=None;
				try:
					ind=attr.index(i);
				except:
					print "Attribute Not found";
					return False;
				if(st_attr[co]!=None):
					st+=str(st_attr[co])+ ' ,';
					co+=1;
					continue;
				if(dis_attr[co]==1):
					try:
						if(line[ind] in rem[i]):
							st=" ";
							break;
						else:
							rem[i].append(line[ind]);
							st+=line[ind] +' ,';
					except:
						rem[i]=[line[ind]];
						st+=line[ind] +' ,';
				else:
					st+=line[ind] +' ,';
			co+=1;
		st= st[:-1];
		if(st!=''):
			print st



def get_static_value(table_name,attribute,attr,func):

	f=open(table_name,'r');
	st=[];
	for line in f:
		line=process_line(line);
		if(line==''):
			continue;
		line=line.split(',');
		ind=attr.index(attribute);
		st.append(int(line[ind]));
	
	if(func=='max'):
		return max(st);
	if(func=='min'):
		return min(st);
	if(func=='sum'):
		return sum(st);
	if(func=='average'):
		return sum(st)*1.0 / len(st);
	






def delete_query(tokens,database):
	tokens=filter_tokens(tokens);
	table_name=tokens[2].value;

	table_attr=[];

	for table in database.table_list:
		if(table.name==table_name):
			table_attr=[table_name+'.'+ j.name for j in table.attr]

	if(len(tokens)<4):
		print "unknown structure"
		return False;
	whereclause=tokens[3];
	f1=open(table_name+'.csv');
	dum_table="dummy";
	f2=open(dum_table+'.csv','w');

	for line in f1:
		line=process_line(line);
		temp_line=line;
		if(line==''):
			continue;
		line=line.split(',');
		if(cond_verify(line,table_attr,whereclause)==False):
			f2.write(temp_line+'\n');
	f2.close();
	os.system("mv "+dum_table+'.csv ' + table_name+'.csv');
	return True;
		







def select_query(tokens,database):
	tokens=filter_tokens(tokens);
	if(len(tokens) < 4):
		return False;


	querywords=tokens[1];

	keyword=tokens[2];
	tablelist=tokens[3];
	whereclause=None;
	cond_clause=[];
	if(len(tokens) ==5):
		whereclause=tokens[4];
	
	try:
		tablelist=[[i.value,i.value] for i in tablelist.get_identifiers()];
	except:
		tablelist=[[tablelist.value,tablelist.value]];
	

	
	#### creating hybrid tables #####

	new_attr=[];

	for k in range(len(tablelist)):
		for i in database.table_list:
			if(i.name==tablelist[k][0]):
				new_attr+=[tablelist[k][1]+'.'+j.name for j in i.attr];
	compound_table="compund_table.csv";
	f1=None;
	try:
		f1=open(tablelist[0][0]+'.csv','r');
	except:
		print "Table :" + tablelist[0][0] + " Not found";
		return False;
	f2=open(compound_table,'w');
	for line in f1:
		line=process_line(line);
		if(line==''):
			continue;
		f2.write(line + '\n');
	f2.close();

	compound_table_semi="compund_table_semi.csv";
	i=1;
	while(i<len(tablelist)):
		f3=open(compound_table_semi,'w')
		f2=open(compound_table,'r');
		for line in f2:
			line=process_line(line);
			if(line==''):
				continue;
			f1=open(tablelist[i][0]+'.csv','r');
			for line1 in f1:
				line1=process_line(line1);
				if(line1==''):
					continue;
				f3.write(line+','+line1+'\n');
			f1.close();
		f3.close();
		f2.close();
		os.system("mv "+ compound_table_semi +' '+ compound_table);
		i+=1;
	
	select_attr=[];
	dis_attr=[];
	st_attr=[];
	try:
		querywords=querywords.get_identifiers();
	except:
		querywords=[querywords];

	semi_attr=[i.split('.')[1] for i in new_attr];
	
	for i in querywords:
		if(i.value=='*'):
			select_attr+=new_attr;
			dis_attr+=[0 for i in new_attr];
			st_attr+=[None for i in new_attr];

		if(isinstance(i,sqlparse.sql.Identifier)):
			attribute=i.value;
			
			if('.' not in attribute and semi_attr.count(attribute)==1):
				ik=semi_attr.index(attribute);
				attribute=new_attr[ik];
			elif('.' not in attribute):
				print "Attribute clash or not found";
				return False;

			select_attr+=[attribute];
			dis_attr+=[0];
			st_attr+=[None];


		if(isinstance(i,sqlparse.sql.Function)):
			function_name=i.tokens[0].value;
			attribute=i.tokens[1].tokens[1].value;

			if('.' not in attribute and semi_attr.count(attribute)==1):
				ik=semi_attr.index(attribute);
				attribute=new_attr[ik];
			elif('.' not in attribute):
				print "Attribute clash or not found";
				return False;

			if(function_name.upper()=='DISTINCT'):
				dis_attr+=[1];
				select_attr+=[attribute]
				st_attr+=[None];

			if(function_name.upper()=='MAX' or function_name.upper()=='MIN' or function_name.upper()=='SUM' or function_name.upper()=="AVERAGE"):
				dis_attr+=[0];
				select_attr+=[attribute]
				value=get_static_value(compound_table,attribute,new_attr,function_name.lower());
				st_attr+=[value];



	select_rows(compound_table,select_attr,dis_attr,st_attr,new_attr,whereclause);
		
	#print select_attr;
	






def process_query(line,database):
	line=process_line(line);
	line=line.split(';')[0];
	parse=sqlparse.parse(line)[0];
	tokens=parse.tokens;
	
	querycheck=tokens[0];
	if(querycheck.value.upper()=="CREATE"):
		create_query(tokens,database);
	
	elif(querycheck.value.upper()=="TRUNCATE"):
		truncate_query(tokens,database);
	
	elif(querycheck.value.upper()=="INSERT"):
		insert_query(tokens,database);
	
	elif(querycheck.value.upper()=="DROP"):
		drop_query(tokens,database);
	
	elif(querycheck.value.upper()=="SELECT"):
		select_query(tokens,database);
	
	elif(querycheck.value.upper()=="DELETE"):
		delete_query(tokens,database);

	else:
		print "unknown command"
	

	

class Database():
	def __init__(self,name):
		self.file_name=name;
		self.table_list=[];
		self.load_database(name);
	
	def add_table_csv(self,name):
		f=open(name+'.csv','w');
		f.close();
	
	def add_table_metadata(self,table_details):
		f=open(self.file_name,'a');
		f.write("\n<begin_table>\n");
		for i in table_details:
			f.write(i+'\n');
		f.write("<end_table>");
		f.close();
		pass;
	
	
	def create_empty_table(self, table_details):
		name=table_details[0];
		attr=table_details[1:];
		attr_type=[ "INT"  for i in attr ];
		flag=0;
		for i in self.table_list:
			if(i.name==name):
				print "Table already exists"
				return False;
		new_table=Table(name,attr,attr_type);
		self.table_list.append(new_table);
		self.add_table_metadata(table_details);
		self.add_table_csv(table_details[0]);
		return True;
		
		
		#self.fill_table(new_table,name+'.csv');



	def create_table(self, table_details):
		name=table_details[0];
		attr=table_details[1:];
		attr_type=[ "INT"  for i in attr ];
		new_table=Table(name,attr,attr_type);
		self.table_list.append(new_table);
		self.fill_table(new_table,name+'.csv');

	def delete_table(self,name):
		os.system("rm "+name+'.csv');
		for i in range(len(self.table_list)):
			if(self.table_list[i].name==name):
				self.table_list=self.table_list[:i]+self.table_list[i+1:];
				self.process_metadata();
	def process_metadata(self):
		open(self.file_name,'w').close();
		for i in self.table_list:
			table_details=[i.name]+[j.name for j in i.attr];
			#print table_details;
			self.add_table_metadata(table_details);
			

	
	def truncate_table(self,name):
		for i in range(len(self.table_list)):
			if(self.table_list[i].name==name):
				table_record=self.table_list[i];
				table_record.data=[];
				open(name+'.csv', 'w').close();
				return True;
		return False;

			

	def fill_table(self,table,name):
		f=open(name);
		for line in f:
			line=process_line(line);
			if(line==''):
				continue;
			line=line.split(',');
			line=[int(i) for i in line];
			table.insert_record(line,0);
		

	def load_database(self,name):
		f=open(name);
		store_flag=0;
		table_details=[];
		for line in f:
			line=process_line(line);
			if(line==''):
				continue;
			if ("<begin_table>" == line and store_flag==0):
				store_flag=1;

			elif("<end_table>" == line and store_flag==1):
				store_flag=0;
				self.create_table(table_details);
				table_details=[];

			elif(store_flag==1):
				table_details.append(line);

			else:
				raise ValueError
		if(store_flag==1):

			raise ValueError




 
class Table():

	def __init__(self,name,attr,attr_type):
		self.name=name;
		self.attr=[];
		self.data=[];
		for i in range(len(attr)):
			new_attr=Attribute(attr[i],attr_type[i]);
			self.attr.append(new_attr);
	
	def insert_record(self,values,flag):
		if(len(self.attr)==len(values)):
			self.data.append(values);
			if(flag==1):
				f=open(self.name+'.csv',"a");
				st='\n';
				for i in values:
					st+=str(i)+',';
				st=st[:-1];
				f.write(st);
				f.close();

			return True;

		else:
			return False;

	def truncate(self):
		self.data=[];

	

class Attribute:

	def __init__(self,name,data_type):
		self.name=name;
		self.data_type=data_type;

if __name__ == "__main__":

	database=Database("metadata.txt");
	while(True):
		print "mysql >> :",
		text=raw_input();
		text=text.split(';');
		for i in text:
			if(i!=''):
				process_query(i,database);
				print "--------------------------------------"


