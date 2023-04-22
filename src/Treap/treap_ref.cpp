//#include<bits/stdc++.h>
#include<stdio.h>
#include<algorithm>
#include<queue>
#include<string.h>
#include<iostream>
#include<math.h>
#include<set>
#include<map>
#include<vector>
#include<iomanip>
using namespace std;
#define ll long long
#define ull unsigned long long
#define pb push_back
#define FOR(a) for(int i=1;i<=a;i++)
const int inf=0x3f3f3f3f;
const int maxn=2e6+7; 
const long long mod=1e9+7;

struct NODE{
	NODE *ch[2];
	int v,r,s,w;//数据，名次，节点大小，数据出现次数
	NODE(int v):v(v){
		ch[0]=ch[1]=NULL;
		r=rand();s=w=1;
	}
	bool operator < (const NODE &rhs)const{return r<rhs.r;}
	int cmp(int x)const{
		if(x==v)return -1;
		return x<v?0:1;		//左边0右边1
	}
	int cmp1(int x)const{	//第k大查询的比较
		int sz=w;
		if(ch[0])sz+=ch[0]->s;
		if(sz-w+1<=x && x<=sz)return -1;//找到自身
		if(x<=sz-w)return 0;
		return 1;
	}
	void maintain(){
		s=w;if(ch[0])s+=ch[0]->s;if(ch[1])s+=ch[1]->s;
	}
}*root;
void rotate(NODE* &o,int d){	//0左旋
	NODE *k=o->ch[d^1];
	o->ch[d^1]=k->ch[d];
	k->ch[d]=o;
	o->maintain();k->maintain();
	o=k;
}
void insert(NODE* &o,int v){
	if(!o){							//空节点
		o=new NODE(v);
		return;
	}else{
		int d=o->cmp(v);
		if(d==-1)o->w++;
		else{
			insert(o->ch[d],v);		//先插到叶子，再往上旋
			if(o->ch[d]>o)rotate(o,d^1);//左儿子大就右旋~
		}
	}
	o->maintain();
}
void del(NODE* &o,int v){
	int d=o->cmp(v);
	if(d==-1){
		if(o->w > 1)o->w--;
		else if(o->ch[0]&&o->ch[1]){
			int d2=0;
			if(o->ch[0]>o->ch[1])d2=1;
			rotate(o,d2);
			del(o->ch[d2],v);
		}else{
			if(o->ch[0])o=o->ch[0];
			else o=o->ch[1];
		}
	}else del(o->ch[d],v);
	if(o)o->maintain();
}
void remove(NODE* &o){
	if(!o)return;
	if(o->ch[0])remove(o->ch[0]);
	if(o->ch[1])remove(o->ch[1]);
	delete o;
	o=NULL;
}
int find(NODE* &o,int x){
	if(o==NULL)return 0;
	int d=o->cmp(x);
	if(d==-1)return o->w;
	return find(o->ch[d],x);
}
int kth(NODE* o,int k){		//按尺寸进子树查找
	int d=o->cmp1(k);
	int sz=o->w;
	if(o->ch[0])sz+=o->ch[0]->s;
	if(d==-1)return o->v;
	if(d==0)return kth(o->ch[0],k);
	return kth(o->ch[1],k-sz);
}
int query(NODE *o,int x){	//求排名,前面有多少个
	if(!o)return 0;
	int d=o->cmp(x);
	int sz=o->w;
	if(o->ch[0])sz+=o->ch[0]->s;
	if(d==-1)return sz-o->w;
	else if(d==0)return query(o->ch[0],x);
	else return query(o->ch[1],x)+sz;
}
/*
//求前驱：
int sz=query(root,x);	
printf("%d\n",kth(root,sz));

//求后继：
int sz=query(root,x);
sz+=find(root,x)+1;
printf("%d\n",kth(root,sz));

int main(){
	insert(root,1);
	insert(root,2);
	insert(root,3);
	cout<<query(root,3)<<endl;
}
*/

int main(){
	int n;scanf("%d",&n);
	int op,x;
	while(n--){
		scanf("%d%d",&op,&x);
		if(op==1){insert(root,x);}
		else if(op==2){
			del(root,x);
		}else if(op==3){
			printf("%d\n",query(root,x)+1);
		}else if(op==4){
			printf("%d\n",kth(root,x));
		}else if(op==5){
			int sz=query(root,x);
			printf("%d\n",kth(root,sz));
		}else if(op==6){
			int sz=query(root,x);
			sz+=find(root,x)+1;
			printf("%d\n",kth(root,sz));
		}
	}
}
