#include<stdio.h>
#include<string.h>
int ans[20]={0};
bool circle[29] = {true};
int k=0,m=0;
int init()
{
    for(k=1;k<=13;k++)
    {
        int n=k+k;
        for(m=k+1;;m++)
        {
            int kill = 0;
            memset(circle, true, sizeof(circle));
            int start = 0;
            while(kill != k)
            {
                int step = m % (n - kill);
                if(step == 0) step = n - kill;
                for(int i=1;i<=step;)
                {
                    start++;
                    if(start>n) start=1;
                    if(circle[start]) i++;
                }
                if(start<=k) break;
                circle[start] = false;
                kill++;
                if(kill==k) break;
                while(circle[start]==false)
                {
                    start--;
                    if(start == 0) start = n;
                }
            }
            if(kill == k) break;
        }
        ans[k]=m;
    }
    return(0);
}
int main()
{
    init();
    while(scanf("%d",&k)!=EOF&&k>0) printf("%d\n",ans[k]);
    return(0);
}

