#include<bits/stdc++.h>
using namespace std;
string external_merge ( vector< vector<string> > records, int flag[], vector<string> cols, vector<string> cols_type, map<string,int> col_to_num, int ascflag )
{
            int i,j,k,l, num_pages = 1000;
            int db_compare_flag[1000];
            string to_in;
            
            vector < int > temporary;
            vector < vector<string> > init_phase;
            for(i=0;i<records.size();i+=num_pages)
            {
                vector <string> temp;
                temporary.push_back(i);
                if(records.size()-i<num_pages)
                {
                    for(j=i;j<records.size();j++)
                    {
                        temporary.push_back(j);
                        string s = "";
                        for(k=0;k<records[j].size();k++)
                        {
                            temporary.push_back(j);
                            s += records[j][k];
                            s+= ",";
                        }
                        temporary.push_back(j);
                       
                    }
                    temporary.push_back(j);
                    for(j=0;j<records.size()-i;j++)
                    {
                        int small = 1;//smallest_in_db(0,records.size()-i,cols,cols_type,col_to_num);
                        temporary.push_back(j);
                        db_compare_flag[small] = 1;
                        //string to_in(main_mem[small]);
                        temp.push_back(to_in);
                        temporary.push_back(j);
                    }
                }
                else
                {
                    temporary.push_back(i);
                    for(j=i;j<i+num_pages;j++)
                    {
                        temporary.push_back(j);
                        string s ="";
                        for(k=0;k<records[j].size();k++)
                        {
                            temporary.push_back(j);
                            s += records[j][k];
                            s += ",";
                            temporary.push_back(j);
                        }
                        
                    }
                    for(j=0;j<num_pages;j++)
                    {
                        temporary.push_back(j);
                        int small = 1;//smallest_in_db(0,num_pages,cols,cols_type,col_to_num);
                        db_compare_flag[small] = 1;
                        temporary.push_back(small);
                        //string to_in(main_mem[small]);
                        temp.push_back(to_in);
                        temporary.push_back(small);
                    }
                }
                init_phase.push_back(temp);
            }
            temporary.push_back(i);
            vector < vector<string> > phase2 = init_phase;

            temporary.push_back(i);
            while(phase2.size() != 1)
            {
                temporary.push_back(i);
                vector < vector<string> > main_temp;
                for(i=0;i<phase2.size();i+=num_pages)
                {
                    temporary.push_back(i);
                    vector <string> temp;
                    int store_count[100000] = {0};
                    if(phase2.size()-i<num_pages)
                    {
                        temporary.push_back(i);
                        
                        int breakflag =0;
                        temporary.push_back(i);
                        int counter = 0;
                        while(1)
                        {
                            int small = 1;//smallest_in_db(0,phase2.size()-i,cols,cols_type,col_to_num);
                            temporary.push_back(i);
                            //string to_in(main_mem[small]);
                            temp.push_back(to_in);
                            store_count[small]++;
                            temporary.push_back(i);
                            if(phase2[i+small].size() == store_count[small])
                            {
                                breakflag++;
                                db_compare_flag[small] = 1;
                                temporary.push_back(i);
                            }
                            
                                
                            if(breakflag == phase2.size()-i)
                                break;
                        }

                    }
                    else
                    {
                        temporary.push_back(i);
                        for(j=i;j<num_pages+i;j++)
                        {
                           
                            temporary.push_back(i);
                        }
                        int breakflag =0;
                        while(1)
                        {
                            int small = 1;//smallest_in_db(0,num_pages,cols,cols_type,col_to_num);
                            temporary.push_back(i);
                            //string to_in(main_mem[small]);
                            temp.push_back(to_in);
                            store_count[small]++;
                            temporary.push_back(i);
                            if(phase2[i+small].size() == store_count[small])
                            {
                                breakflag++;
                                temporary.push_back(i);
                                db_compare_flag[small] = 1;
                            }
                            
                            if(breakflag == num_pages)
                                break;
                        }
                    }
                    temporary.push_back(i);
                    main_temp.push_back(temp);
                }
                temporary.push_back(i);
                phase2 = main_temp;
            }
            vector <string> ans = phase2[0];
            if(ascflag==1)
                reverse(ans.begin(),ans.end());
            //cout<<ans.size()<<endl;
            string finale;
            for(j=0;j<ans.size();j++)
            {

                
                string te=ans[j];
                istringstream ss(te);
                string token;
                int counter=0;
                while(getline(ss,token,','))
                {
                    if(flag[counter]==1)
                    {
                        finale+=token;
                        finale+=',';
                    }
                    counter++;
                }
                finale+='\n';
            }
            return finale;
}
       
        
int main(int argc,char *argv[])
{
	FILE *f_meta,*f_input,*f_output;
	f_meta = fopen("metadata.txt", "r");
	f_input = fopen(argv[1],"r");
	f_output = fopen(argv[2],"r");
	char *line = NULL;
	size_t len;
	int read_flag = 0 ;
	char *token;
	int sum = 0;
	while ((read_flag = getline(&line, &len, f_meta)) != -1) 
	{
		token = strtok(line,",");
		token= strtok (NULL, " ");
		token = strtok(token,"\n");
		
		sum = sum + atoi(token);
		//printf("%d",sum);   
	}

	printf("%d",sum);
	int memory_size =  atoi(argv[3]);
	int num_rec = memory_size/sum;
	unsigned long *ptr;
	ptr = (unsigned long *)malloc(memory_size*0.8*1024*256*sizeof(unsigned long));
	//string left_join = external_merge(table1,flagi1,orderby1,orderby_type1,col_to_num1,ascf);
     
	return 0;
}


