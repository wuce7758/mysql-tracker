#include<stdio.h>
#include<map>
#include<string>
#include<iostream>
using namespace std;
int t=0;
int sum_day=0;
map<string,int> haab_map;
map<int,string> tzolkin_map;
char buf[100];
int init_map()
{
    haab_map["pop"]=1;//month begin with 1
    haab_map["no"]=2;
    haab_map["zip"]=3;
    haab_map["zotz"]=4;
    haab_map["tzec"]=5;
    haab_map["xul"]=6;
    haab_map["yoxkin"]=7;
    haab_map["mol"]=8;
    haab_map["chen"]=9;
    haab_map["yax"]=10;
    haab_map["zac"]=11;
    haab_map["ceh"]=12;
    haab_map["mac"]=13;
    haab_map["kankin"]=14;
    haab_map["muan"]=15;
    haab_map["pax"]=16;
    haab_map["koyab"]=17;
    haab_map["cumhu"]=18;
    haab_map["uayet"]=19;
    tzolkin_map[0]="ahau";
    tzolkin_map[1]="imix"; //day begin with 1 number is from 1 to 13 cycle  name is 1 to 20 cycle
    tzolkin_map[2]="ik";
    tzolkin_map[3]="akbal";
    tzolkin_map[4]="kan";
    tzolkin_map[5]="chicchan";
    tzolkin_map[6]="cimi";
    tzolkin_map[7]="manik";
    tzolkin_map[8]="lamat";
    tzolkin_map[9]="muluk";
    tzolkin_map[10]="ok";
    tzolkin_map[11]="chuen";
    tzolkin_map[12]="eb";
    tzolkin_map[13]="ben";
    tzolkin_map[14]="ix";
    tzolkin_map[15]="mem";
    tzolkin_map[16]="cib";
    tzolkin_map[17]="caban";
    tzolkin_map[18]="eznab";
    tzolkin_map[19]="canac";
    tzolkin_map[20]="ahau";
    return(0);
}
int main()
{
    init_map();
    scanf("%d",&t);
    printf("%d\n",t);
    while(t--)
    {
        int haab_day=0,haab_year=0,tzolkin_num,tzolkin_year;
        string haab_month;
        string tzolkin_name;
        scanf("%d.%s%d",&haab_day,buf,&haab_year);
        haab_month = buf;
        sum_day=(haab_year*365) + (haab_map[haab_month] - 1) * 20 + (haab_day + 1);
        tzolkin_year = sum_day / 260;
        if(sum_day % 260 == 0) tzolkin_year--;
        tzolkin_name = tzolkin_map[sum_day % 20];
        tzolkin_num = sum_day % 13;
        if(tzolkin_num == 0) tzolkin_num = 13;
        printf("%d %s %d\n",tzolkin_num, tzolkin_name.c_str(), tzolkin_year);
    }
    return(0);
}
