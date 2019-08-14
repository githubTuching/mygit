#_*_coding:UTF-8_*_
from django.shortcuts import render


from impala.dbapi import connect
from urllib.parse import parse_qs
from django.http import HttpResponse
from django.http.response import JsonResponse


 
def hello(request):                   #此函数增加
    user=[]
    for i in range(20):
        user.append(i)
    return render(request,'hello.html')

def login2(request,h):

    conn = connect(host='130.10.7.108', port=21050)
    cursor = conn.cursor()
    cursor.execute('USE rk')
    p=int(h)
    a=cursor.execute('select xzqhlb from rk.DIM_RY_XZQH where xzqhlb like concat("%_朝阳区_%") limit %s',[p])
    print(type(a))
    results1 = cursor.fetchall()
    print(results1)
    cursor.execute('select x.xzqhlb,x1.xblb,w.WHCDLB from rk.fact_data_ry f left join rk.DIM_RY_XB x1 on f.XBid=x1.xbid left join rk.DIM_RY_WHCD w on f.whcdid=w.whcdid left join rk.DIM_RY_XZQH x on f.xzqhid=x.xzqhid where xzqhlb like concat("%_朝阳区_%") and xblb="男" limit 10')

    results2 = cursor.fetchall()
    print (results2)
    return JsonResponse(results2,safe=False)

def login(request):
    list_data = [1,2,3,4]
    return JsonResponse(list_data,safe=False)

def params_post(request):
    if request.method=='GET':
        return render(request,'post.html')
    else:
        name=request.POST.get('name','')
        item=request.POST.get('item','')
        conn = connect(host='130.10.7.108', port=21050)
        cursor = conn.cursor()
        cursor.execute('USE rk')
        a=cursor.execute('select x.xzqhlb,x1.xblb,w.WHCDLB from rk.fact_data_ry f left join rk.DIM_RY_XB x1 on f.XBid=x1.xbid left join rk.DIM_RY_WHCD w on f.whcdid=w.whcdid left join rk.DIM_RY_XZQH x on f.xzqhid=x.xzqhid where xzqhlb like concat("%_朝阳区_%") and xblb=%s limit 5',[name])
        print(type(a))
        results1 = cursor.fetchall()
        print(results1)
        dict_list={'code':0,'msg':'请求成功','status':200,'result1':results1}
        return JsonResponse(dict_list,safe=False)

def test(request):
    sqlParam = []
    conn = connect(host='130.10.7.108', port=21050)
    cursor = conn.cursor()
    cursor.execute('USE rk')
    sql = 'select x.xzqhlb,x1.xbbs,w.WHCDBS  from rk.fact_data_ry f left join rk.DIM_RY_XB x1 on f.xbid=x1.xbid left join rk.DIM_RY_WHCD w on f.whcdid=w.whcdid left join rk.DIM_RY_XZQH x on f.xzqhid=x.xzqhid'
    
    a=cursor.execute(sql)
    results1 = cursor.fetchall()
    return JsonResponse(result1,safe=False)
