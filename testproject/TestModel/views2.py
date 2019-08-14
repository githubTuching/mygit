from django.shortcuts import render


from impala.dbapi import connect
from urllib.parse import parse_qs
from django.http import HttpResponse
from django.http.response import JsonResponse
from django.http import QueryDict 
import json
import logging
import operator



# Create your views here.
def index(request):
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
        return render(request,"TestModel/hello.html",{"hello":results1})

def search(request):
    if request.method == 'GET':
        return render(request,"TestModel/hello.html",{"hello":"pass"})
    else:
        ret={
            "name":"adsf"
        }
        q = QueryDict('a=1&a=2&a=3')
        q.lists()
        test = request.POST
        print(test)
        print(type(test))
        a2 = '{"status":0,"message":"ok","results":[{"name":"park","location":{"lat":39.498402,"lng":116.007069},"address":"xx road","street_id":"32541349605e7ae96ca3cc1e","detail":1,"uid":"32541349605e7ae96ca3cc1e"}]}'
        jsonData = json.loads(a2)
        print(jsonData)
        print(jsonData['results'][0]['location']['lat'])
        a=request.body
        data = json.loads(a)
        print(data["dim_info"]["xb"])
        print(type(data["dim_info"]["xb"]))
        print(data.values)
        sql=doJoinSql(data)
        print(sql)
        conn = connect(host='130.10.7.108', port=21050)
        cursor = conn.cursor()
        cursor.execute('USE rk')
        a=cursor.execute(sql)
        print(type(a))
        results1 = cursor.fetchall()
        print(results1)
        
        #return render(request,"TestModel/hello.html",{"hello":results1})
        #return render(request,"TestModel/hello.html",{"hello":"pass"})    
        return JsonResponse(results1,safe=False)
        #return HttpResponse(json.dumps(ret),content_type="application/json")

def search2(request):
    if request.method == 'GET':
        return render(request,"TestModel/hello.html",{"hello":"pass"})
    else:
        a=request.body
        data = json.loads(a)
        print(data)
        sql=doJoinSql7(data)
        print(sql)
        conn = connect(host='130.10.7.108', port=21050)
        cursor = conn.cursor()
        cursor.execute('USE rk')
        a=cursor.execute(sql)
        print(type(a))
        results1 = cursor.fetchall()
        print(results1)
        print(type(results1))
        ret={
            'msg': testmsg(results1),
            'code': testcode(results1),
            'data': testlen2(data,results1)
            }
        return JsonResponse(ret,safe=False)
        #return render(request,"TestModel/hello.html",{"hello":results1})
        #return render(request,"TestModel/hello.html",{"hello":"pass"})    
        #return HttpResponse(json.dumps(data),content_type="application/json")

def search3(request):
    if request.method == 'GET':
        return render(request,"TestModel/hello.html",{"hello":"pass"})
    else:
        a=request.body
        data = json.loads(a)
        print(data)
        sql=doJoinSql5(data)
        print(sql)
        conn = connect(host='130.10.7.108', port=21050)
        cursor = conn.cursor()
        cursor.execute('USE rk')
        a=cursor.execute(sql)
        print(type(a))
        results1 = cursor.fetchall()
        print(results1)
        print(type(results1))
        try:
            ret={
                'msg': testmsg(results1),
                'code': testcode(results1),
                'data': testlen2(data,results1)
                }
            return JsonResponse(ret,safe=False)
        except Error as Argument:
            print("没有符合以上条件")
        finally:
            print("没有符合以上条件")

def testlen(dict_temp,results1):
    str_theme = dict_temp["theme"]
    rkey=""
    rval=""
    two = {}
    twices=[""]
    tr_dict={}
    m=-1    
    for i in range(0,len(results1)):
        for j in range(0,len(results1[i])):
            for dim in dict_temp["dim_info"].keys():
                m+=1
                if m < j:
                    continue
                else:
                    two =two.update(dim=results1[i][j])
                    m=-1
                    break
        twices=twices.append(two).append(",")
    return twices
           
def testlen2(dict_temp,results1):
    str_theme = dict_temp["theme"]
    rkey=""
    rval=""
    hal=""
    m=-1
    dict_temp["dim_info"]["count"]=""
    for i in range(0,len(results1)):
        for j in range(0,len(results1[i])):
            for dim in dict_temp["dim_info"].keys():
                m+=1              
                if m < j:
                    continue
                #依据返回的数据以组为单位组装数据                
                rkey=rkey+'"'+dim+'"'+":"+'"'+str(results1[i][j])+'"'+", "
                m=-1
                break
        rval=rval+"{"+rkey+"}"+", "
        rkey=""
    hal="{"+rval+"}"
    return hal

def testlen3(dict_temp,results1):
    str_theme = dict_temp["theme"]
    rkey=""
    rval=""
    two = {}
    twices=[]
    tr_dict={}
    m=-1
    dict_temp["dim_info"]["count"]=""
    for i in range(0,len(results1)):
        for j in range(0,len(results1[i])):
            for dim in dict_temp["dim_info"].keys():
                m+=1
                if m < j:
                    continue
                #依据返回的数据以组为单位组装数据                
                #rkey=rkey+'"'+dim+'"'+":"+'"'+str(results1[i][j])+'"'+", "
                
                if dim=="xzqh":
                    trs=list(results1[i])
                    #print(trs)
                    #print(type(trs))
                    for elenum in range(0,len(dict_temp["dim_info"][dim])):
                        if dict_temp["dim_info"][dim][elenum] in trs[j]:
                            trs[j]=dict_temp["dim_info"][dim][elenum]
                        else:
                            continue
                    #trs[j]=trs[j]
                    #print(trs)
                    twices.append(trs)
                    # print(twices)
                    rkey=rkey+'"'+dim+'"'+":"+'"'+str(results1[i][j])+'"'+", "
                m=-1
                break
        rval=rval+"{"+rkey+"}"+", "
        rkey=""
    hal="{"+rval+"}"
    sum=twices[0][-1]            
    for num2 in range(1,len(twices)):
        if operator.eq(twices[0][0:-1],twices[num2][0:-1]):
            sum+=int(twices[num2][-1])
    for num1_1 in range(1,len(twices)):
        if operator.eq(twices[num1_1][0:-1],twices[0][0:-1]):
            del(twices[num1_1])
        if twices:
            pass
        else:
            break
    del(twices[0])
            
            

        #print("----------------------------------------------------------------------------------------------------")
        #print(list_o)
        #print(type(list_o))
        #print("----------------------------------------------------------------------------------------------------")

    print(twices)
    print(sum)
    #print(type(twices))
    return twices


def testmsg(results1):
    msg=""
    if results1 is not None:
        msg="请求成功"
    else:
        msg="请求失败"
    return msg
def testcode(results1):
    code=-1
    if results1 is not None:
        code=0
    else:
        code=1
    return code

    
def doJoinSql(dict_temp):
    dict_unjoin_data = dict_temp

    str_theme = dict_unjoin_data["theme"]
    #事实表信息
    #str_fact_info = "fact_"+str_theme  
    str_fact_info = "fact_data_"+str_theme
    #拼接“select XXX”部分
    str_select_para = ""
    #拼接“left join”部分
    str_left_join_para = ""
    str0=""
    str=""
    aa=""
    strss=""
    for dim in dict_unjoin_data["dim_info"].keys():
        #维表信息
        str_dim_info = "dim_"+str_theme+"_"+dim
        str_dim_id_info = dim+"id"
        str_select_para = str_select_para+dim+"lb,"
        str_left_join_para = str_left_join_para+" left join " +str_dim_info+" on "+str_fact_info+"."+ str_dim_id_info+"="+ str_dim_info+"."+str_dim_id_info
        if dim=="xzqh":
            length=len(dict_unjoin_data["dim_info"][dim])
            for j in range(0,length):
                if length==1:
                    str0=dict_unjoin_data["dim_info"][dim][j]
                else:
                    if j==length-1:
                        str0=str0+dict_unjoin_data["dim_info"][dim][j]
                        break

                    str0=dict_unjoin_data["dim_info"][dim][j]+"|"+str0
            str0="'"+str0+"'"
            aa=aa+dim+"lb"+" regexp "+str0+" and "
            #print(str0)
        else:
            #获取dim字段的key对应的value内容
            for i in dict_unjoin_data["dim_info"][dim]:

                str=str+"'"+i+"'"+","
        
            print("str="+str)
        
            temp =str.split(",")
            #去掉dim字段的key对应的value内容序列最后一个,
            temp.pop()
            str_para =",".join(temp)
            str_paras="("+str_para+")"
            aa=aa+dim+"lb"+" in "+str_paras +" and "
        strss=strss+dim+"lb,"
        str=""
    temp2=strss.split(",")
    temp2.pop()
    str_para2 =",".join(temp2)
    str_left_join_para += " where "+aa+" 1=1 "
    sql = "select "+str_select_para +"count(1) "+"from "+str_fact_info+str_left_join_para+" group by "+str_para2+" order by "+str_para2
    return sql

def doJoinSql2(dict_temp):
    dict_unjoin_data = dict_temp

    str_theme = dict_unjoin_data["theme"]
    #事实表信息
    #str_fact_info = "fact_"+str_theme  
    str_fact_info = "fact_data_"+str_theme
    #拼接“select XXX”部分
    str_select_para = ""
    #拼接“left join”部分
    str_left_join_para = ""
    #拼接“xzqh”的查询参数值
    str_xzqh_query_para=""
    #拼接where条件参数值
    str_dim_value=""
    #拼接where查询条件参数语句部分
    str_condition_join_para=""
    #拼接groupby、orderby后面的条件参数
    str_dimlb_join_para=""
    for dim in dict_unjoin_data["dim_info"].keys():
        #维表信息
        str_dim_info = "dim_"+str_theme+"_"+dim
        str_dim_id_info = dim+"id"
        str_select_para = str_select_para+dim+"lb,"
        str_left_join_para = str_left_join_para+" left join " +str_dim_info+" on "+str_fact_info+"."+ str_dim_id_info+"="+ str_dim_info+"."+str_dim_id_info
        if dim=="xzqh":
            length=len(dict_unjoin_data["dim_info"][dim])
            for j in range(0,length):
                if length==1:
                    str_xzqh_query_para=dict_unjoin_data["dim_info"][dim][j]
                else:
                    if j==length-1:
                        str_xzqh_query_para=str_xzqh_query_para+dict_unjoin_data["dim_info"][dim][j]
                        break

                    str_xzqh_query_para=dict_unjoin_data["dim_info"][dim][j]+"|"+str_xzqh_query_para
            str_xzqh_query_para="'"+str_xzqh_query_para+"'"
            str_condition_join_para=str_condition_join_para+dim+"lb"+" regexp "+str_xzqh_query_para+" and "
            #print(str_xzqh_query_para)
        else:
            #获取dim字段的key对应的value内容
            for i in dict_unjoin_data["dim_info"][dim]:

                str_dim_value=str_dim_value+"'"+i+"'"+","

            print("str_dim_value="+str_dim_value)

            temp =str_dim_value.split(",")
            #去掉dim字段的key对应的value内容序列最后一个,
            temp.pop()
            str_para =",".join(temp)
            str_paras="("+str_para+")"
            str_condition_join_para=str_condition_join_para+dim+"lb"+" in "+str_paras +" and "
        str_dimlb_join_para=str_dimlb_join_para+dim+"lb,"
        str_dim_value=""
    temp2=str_dimlb_join_para.split(",")
    temp2.pop()
    str_para2 =",".join(temp2)
    str_left_join_para += " where "+str_condition_join_para+" 1=1 "
    sql = "select "+str_select_para +"count(1) "+"from "+str_fact_info+str_left_join_para+" group by "+str_para2+" order by "+str_para2
    return sql



def doJoinSql3(dict_temp):
    dict_unjoin_data = dict_temp

    str_theme = dict_unjoin_data["theme"]
    #事实表信息
    #str_fact_info = "fact_"+str_theme  
    str_fact_info = "fact_data_"+str_theme
    #拼接“select XXX”部分
    str_select_para = ""
    #拼接“left join”部分
    str_left_join_para = ""
    #拼接“xzqh”的查询参数值
    str_xzqh_query_para=""
    #拼接where条件参数值
    str_dim_value=""
    #拼接where查询条件参数语句部分
    str_condition_join_para=""
    #拼接groupby、orderby后面的条件参数
    str_dimlb_join_para=""
    for dim in dict_unjoin_data["dim_info"].keys():
        #维表信息
        str_dim_info = "dim_"+str_theme+"_"+dim
        str_dim_id_info = dim+"id"
        str_select_para = str_select_para+dim+"lb,"
        str_left_join_para = str_left_join_para+" left join " +str_dim_info+" on "+str_fact_info+"."+ str_dim_id_info+"="+ str_dim_info+"."+str_dim_id_info
        if dim=="xzqh":
            length=len(dict_unjoin_data["dim_info"][dim])
            for j in range(0,length):
                str_dim_element_list=dict_unjoin_data["dim_info"][dim][j].split('_')
                if length==1:
                    str_xzqh_query_para=str_dim_element_list[-1]
                    #str_xzqh_query_para=dict_unjoin_data["dim_info"][dim][j]
                else:
                    if j==length-1:
                        str_xzqh_query_para=str_xzqh_query_para+str_dim_element_list[-1]
                        break

                    str_xzqh_query_para=str_dim_element_list[-1]+"|"+str_xzqh_query_para
            str_xzqh_query_para="'"+str_xzqh_query_para+"'"
            str_condition_join_para=str_condition_join_para+dim+"lb"+" regexp "+str_xzqh_query_para+" and "
            #print(str_xzqh_query_para)
        else:
            #获取dim字段的key对应的value内容
            for i in dict_unjoin_data["dim_info"][dim]:

                str_dim_value=str_dim_value+"'"+i+"'"+","

            print("str_dim_value="+str_dim_value)

            temp =str_dim_value.split(",")
            #去掉dim字段的key对应的value内容序列最后一个,
            temp.pop()
            str_para =",".join(temp)
            str_paras="("+str_para+")"
            str_condition_join_para=str_condition_join_para+dim+"lb"+" in "+str_paras +" and "
        str_dimlb_join_para=str_dimlb_join_para+dim+"lb,"
        str_dim_value=""
    temp2=str_dimlb_join_para.split(",")
    temp2.pop()
    str_para2 =",".join(temp2)
    str_left_join_para += " where "+str_condition_join_para+" 1=1 "
    sql = "select "+str_select_para +"count(1) "+"from "+str_fact_info+str_left_join_para+" group by "+str_para2+" order by "+str_para2
    return sql
  

def doJoinSql4(dict_temp):
    dict_unjoin_data = dict_temp

    str_theme = dict_unjoin_data["theme"]
    #事实表信息
    #str_fact_info = "fact_"+str_theme  
    str_fact_info = "fact_data_"+str_theme
    #拼接“select XXX”部分
    str_select_para = ""
    #拼接“left join”部分
    str_left_join_para = ""
    #截取“xzqh”的参数后的值
    str_dim_element_list=[]
    #拼接“xzqh”的查询参数值
    str_xzqh_query_para=""
    #拼接province条件参数值
    str_dim_elements_para1=""
    #拼接qx条件参数值
    str_dim_elements_para2=""
    #拼接strict条件参数值
    str_dim_elements_para3=""
    #拼接community条件参数值
    str_dim_elements_para4=""
    #拼接where条件参数值
    str_dim_value=""
    #拼接where查询条件参数语句部分
    str_condition_join_para=""
    #拼接groupby、orderby后面的条件参数
    str_dimlb_join_para=""
    str_dim_elements_lists=[]
    #拼接province查询参数值
    str_xzqh_queryss_parass1=""
    #拼接qx查询参数值
    str_xzqh_queryss_parass2=""
    #拼接strict查询参数值
    str_xzqh_queryss_parass3=""
    #拼接community查询参数值
    str_xzqh_queryss_parass4=""
    try:
        for dim in dict_unjoin_data["dim_info"].keys():
            #维表信息
            str_dim_info = "dim_"+str_theme+"_"+dim
            str_dim_id_info = dim+"id"
            #str_select_para = str_select_para+dim+"lb,"
            str_left_join_para = str_left_join_para+" left join " +str_dim_info+" on "+str_fact_info+"."+ str_dim_id_info+"="+ str_dim_info+"."+str_dim_id_info
            if dim=="xzqh":
                length=len(dict_unjoin_data["dim_info"][dim])
                for j in range(0,length):
                    str_dim_element_list=dict_unjoin_data["dim_info"][dim][j].split('_')
                    print(str_dim_element_list)
                    print(len(str_dim_element_list))

                    str_xzqh_query_para=str_dim_element_list[-1]

                    str_dim_elements_lists.append(str_dim_element_list)


                    if len(str_dim_element_list)==1:
                        str_xzqh_queryss_parass1+="'"+str_xzqh_query_para+"'"+","
                    elif len(str_dim_element_list)==2:
                        str_xzqh_queryss_parass2+="'"+str_xzqh_query_para+"'"+","
                    elif len(str_dim_element_list)==3:
                        str_xzqh_queryss_parass3+="'"+str_xzqh_query_para+"'"+","
                    elif len(str_dim_element_list)==4:
                        str_xzqh_queryss_parass4+="'"+str_xzqh_query_para+"'"+","
                    else:
                        print("bksx")

                if str_xzqh_queryss_parass1:
                    temp3=str_xzqh_queryss_parass1.split(",")
                    temp3.pop()
                    str_dim_element_para1=",".join(temp3)
                    str_dim_elements_para1="("+str_dim_element_para1+")"
                elif str_xzqh_queryss_parass2:
                    temp4=str_xzqh_queryss_parass2.split(",")
                    temp4.pop()
                    str_dim_element_para2=",".join(temp4)
                    str_dim_elements_para2="("+str_dim_element_para2+")"
                elif str_xzqh_queryss_parass3:
                    temp5=str_xzqh_queryss_parass3.split(",")
                    temp5.pop()
                    str_dim_element_para3=",".join(temp5)
                    str_dim_elements_para3="("+str_dim_element_para3+")"
                elif str_xzqh_queryss_parass4:
                    temp6=str_xzqh_queryss_parass4.split(",")
                    temp6.pop()
                    str_dim_element_para4=",".join(temp6)
                    str_dim_elements_para4="("+str_dim_element_para4+")"
                else:
                    print("没有符合以上条件")
                print("------------------------------------------------------------------------------------------------------")
                #print(str_dim_elements_para)
                print("------------------------------------------------------------------------------------------------------")
                #str_dim_elements_lists.append(str_dim_element_list)
                print(str_dim_elements_lists)
                for list_num in range(0,len(str_dim_elements_lists)):
                    if len(str_dim_elements_lists[list_num])==1:
                        if "provincelb" not in str_condition_join_para:
                            str_condition_join_para=str_condition_join_para+"provincelb"+" in "+str_dim_elements_para1+" and "
                        if "provincelb" not in str_select_para:
                            str_select_para = str_select_para+"provincelb,"
                            str_dimlb_join_para=str_dimlb_join_para+"provincelb,"
                    elif len(str_dim_elements_lists[list_num])==2:
                        if "qxlb" not in str_condition_join_para:
                            str_condition_join_para=str_condition_join_para+"qxlb"+" in "+str_dim_elements_para2+" and "
                        if "qxlb" not in str_select_para:
                            str_select_para = str_select_para+"qxlb,"
                            str_dimlb_join_para=str_dimlb_join_para+"qxlb,"
                    elif len(str_dim_elements_lists[list_num])==3:
                        if "strictlb" not in str_condition_join_para:
                            str_condition_join_para=str_condition_join_para+"strictlb"+" in "+str_dim_elements_para3+" and "
                        if "strictlb" not in str_select_para:
                            str_select_para = str_select_para+"strictlb,"
                            str_dimlb_join_para=str_dimlb_join_para+"strictlb,"
                    else:
                        if "communitylb" not in str_condition_join_para:
                            str_condition_join_para=str_condition_join_para+"communitylb"+" in "+str_dim_elements_para4+" and "
                        if "communitylb" not in str_select_para:
                            str_select_para = str_select_para+"communitylb,"
                            str_dimlb_join_para=str_dimlb_join_para+"communitylb,"
                print(str_condition_join_para)
                #print(str_xzqh_query_para)


            else:
                str_select_para = str_select_para+dim+"lb,"
                #获取dim字段的key对应的value内容
                for i in dict_unjoin_data["dim_info"][dim]:

                    str_dim_value=str_dim_value+"'"+i+"'"+","

                print("str_dim_value="+str_dim_value)
                temp =str_dim_value.split(",")
                #去掉dim字段的key对应的value内容序列最后一个,
                temp.pop()
                str_para =",".join(temp)
                str_paras="("+str_para+")"
                str_condition_join_para=str_condition_join_para+dim+"lb"+" in "+str_paras +" and "
                str_dimlb_join_para=str_dimlb_join_para+dim+"lb,"
            str_dim_value=""
        temp2=str_dimlb_join_para.split(",")
        temp2.pop()
        str_para2 =",".join(temp2)
        str_left_join_para += " where "+str_condition_join_para+" 1=1 "
        sql = "select "+str_select_para +"count(1) "+"from "+str_fact_info+str_left_join_para+" group by "+str_para2+" order by "+str_para2
        return sql   
    except Exception as e:
        print(e)
        

def doJoinSql5(dict_temp):
    dict_unjoin_data = dict_temp

    str_theme = dict_unjoin_data["theme"]
    #事实表信息
    #str_fact_info = "fact_"+str_theme  
    str_fact_info = "fact_data_"+str_theme
    #拼接“select XXX”部分
    str_select_para = ""
    #拼接“left join”部分
    str_left_join_para = ""
    #截取“xzqh”的参数后的值
    str_dim_element_list=[]
    #拼接“xzqh”的查询参数值
    str_xzqh_query_para=""
    #拼接province条件参数值
    str_dim_elements_para1=""
    #拼接qx条件参数值
    str_dim_elements_para2=""
    #拼接strict条件参数值
    str_dim_elements_para3=""
    #拼接community条件参数值
    str_dim_elements_para4=""
    #拼接where条件参数值
    str_dim_value=""
    #拼接where查询条件参数语句部分
    str_condition_join_para=""
    #拼接groupby、orderby后面的条件参数
    str_dimlb_join_para=""
    str_dim_elements_lists=[]
    #拼接province查询参数值
    str_xzqh_queryss_parass1=""
    #拼接qx查询参数值
    str_xzqh_queryss_parass2=""
    #拼接strict查询参数值
    str_xzqh_queryss_parass3=""
    #拼接community查询参数值
    str_xzqh_queryss_parass4=""
    for dim in dict_unjoin_data["dim_info"].keys():
        #维表信息
        str_dim_info = "dim_"+str_theme+"_"+dim
        str_dim_id_info = dim+"id"
        #str_select_para = str_select_para+dim+"lb,"
        #str_left_join_para = str_left_join_para+" left join " +str_dim_info+" on "+str_fact_info+"."+ str_dim_id_info+"="+ str_dim_info+"."+str_dim_id_info
        if dim=="day":
            pass
        else:
            str_left_join_para = str_left_join_para+" left join " +str_dim_info+" on "+str_fact_info+"."+ str_dim_id_info+"="+ str_dim_info+"."+str_dim_id_info
        if dim=="xzqh":
            length=len(dict_unjoin_data["dim_info"][dim])
            for j in range(0,length):
                str_dim_element_list=dict_unjoin_data["dim_info"][dim][j].split('_')
                print(str_dim_element_list)
                print(len(str_dim_element_list))
                
                str_xzqh_query_para=str_dim_element_list[-1]

                str_dim_elements_lists.append(str_dim_element_list)
                   
                
                if len(str_dim_element_list)==1:
                    str_xzqh_queryss_parass1+="'"+str_xzqh_query_para+"'"+","
                elif len(str_dim_element_list)==2:
                    str_xzqh_queryss_parass2+="'"+str_xzqh_query_para+"'"+","
                elif len(str_dim_element_list)==3:
                    str_xzqh_queryss_parass3+="'"+str_xzqh_query_para+"'"+","
                elif len(str_dim_element_list)==4:
                    str_xzqh_queryss_parass4+="'"+str_xzqh_query_para+"'"+","
                else:
                    print("bksx")

            if str_xzqh_queryss_parass1:
                temp3=str_xzqh_queryss_parass1.split(",")        
                temp3.pop()
                str_dim_element_para1=",".join(temp3)
                str_dim_elements_para1="("+str_dim_element_para1+")"
            if str_xzqh_queryss_parass2:
                temp4=str_xzqh_queryss_parass2.split(",")
                temp4.pop()
                str_dim_element_para2=",".join(temp4)
                str_dim_elements_para2="("+str_dim_element_para2+")"
            if str_xzqh_queryss_parass3:
                temp5=str_xzqh_queryss_parass3.split(",")
                temp5.pop()
                str_dim_element_para3=",".join(temp5)
                str_dim_elements_para3="("+str_dim_element_para3+")"
                print(str_dim_elements_para3)
            if str_xzqh_queryss_parass4:
                temp6=str_xzqh_queryss_parass4.split(",")
                temp6.pop()
                str_dim_element_para4=",".join(temp6)
                str_dim_elements_para4="("+str_dim_element_para4+")"
            else:
                print("没有符合以上条件")
            print("------------------------------------------------------------------------------------------------------")
            #print(str_dim_elements_para)
            print("------------------------------------------------------------------------------------------------------")
            #str_dim_elements_lists.append(str_dim_element_list)
            print(str_dim_elements_lists)
            for list_num in range(0,len(str_dim_elements_lists)):
                if len(str_dim_elements_lists[list_num])==1:
                    if "provincelb" not in str_condition_join_para:
                        str_condition_join_para=str_condition_join_para+"provincelb"+" in "+str_dim_elements_para1+" and "
                    if "provincelb" not in str_select_para:
                        str_select_para = str_select_para+"provincelb,"
                        str_dimlb_join_para=str_dimlb_join_para+"provincelb,"
                elif len(str_dim_elements_lists[list_num])==2:
                    if "qxlb" not in str_condition_join_para:
                        str_condition_join_para=str_condition_join_para+"qxlb"+" in "+str_dim_elements_para2+" and "
                    if "qxlb" not in str_select_para:
                        str_select_para = str_select_para+"qxlb,"
                        str_dimlb_join_para=str_dimlb_join_para+"qxlb,"
                elif len(str_dim_elements_lists[list_num])==3:
                    if "strictlb" not in str_condition_join_para:
                        str_condition_join_para=str_condition_join_para+"strictlb"+" in "+str_dim_elements_para3+" and "
                    if "strictlb" not in str_select_para:
                        str_select_para = str_select_para+"strictlb,"
                        str_dimlb_join_para=str_dimlb_join_para+"strictlb,"
                else:
                    if "communitylb" not in str_condition_join_para:
                        str_condition_join_para=str_condition_join_para+"communitylb"+" in "+str_dim_elements_para4+" and "
                    if "communitylb" not in str_select_para:
                        str_select_para = str_select_para+"communitylb,"
                        str_dimlb_join_para=str_dimlb_join_para+"communitylb,"

            print(str_condition_join_para)
            #print(str_xzqh_query_para)


        else:
            if dim=="day":
                str_select_para = str_select_para+"day,"
            else:
                str_select_para = str_select_para+dim+"lb,"
            #str_select_para = str_select_para+dim+"lb,"
            #获取dim字段的key对应的value内容
            for i in dict_unjoin_data["dim_info"][dim]:

                str_dim_value=str_dim_value+"'"+i+"'"+","

            print("str_dim_value="+str_dim_value)
            temp =str_dim_value.split(",")
            #去掉dim字段的key对应的value内容序列最后一个,
            temp.pop()
            str_para =",".join(temp)
            str_paras="("+str_para+")"
            #str_condition_join_para=str_condition_join_para+dim+"lb"+" in "+str_paras +" and "
            #str_dimlb_join_para=str_dimlb_join_para+dim+"lb,"
            if dim=="day":
                str_condition_join_para=str_condition_join_para+dim+" in "+str_paras +" and "
                str_dimlb_join_para=str_dimlb_join_para+"day,"
            else:
                str_condition_join_para=str_condition_join_para+dim+"lb"+" in "+str_paras +" and "
                str_dimlb_join_para=str_dimlb_join_para+dim+"lb,"
        str_dim_value=""
    temp2=str_dimlb_join_para.split(",")
    temp2.pop()
    str_para2 =",".join(temp2)
    str_left_join_para += " where "+str_condition_join_para+" 1=1 "
    sql = "select "+str_select_para +"count(1) "+"from "+str_fact_info+str_left_join_para+" group by "+str_para2+" order by "+str_para2
    return sql

def doJoinSql6(dict_temp):
    dict_unjoin_data = dict_temp

    str_theme = dict_unjoin_data["theme"]
    #事实表信息
    #str_fact_info = "fact_"+str_theme  
    str_fact_info = "fact_data_"+str_theme
    #拼接“select XXX”部分
    str_select_para = ""
    #拼接“left join”部分
    str_left_join_para = ""
    #截取“xzqh”的参数后的值
    str_dim_element_list=[]
    #拼接“xzqh”的查询参数值
    str_xzqh_query_para=""
    #拼接province条件参数值
    str_dim_elements_para1=""
    #拼接qx条件参数值
    str_dim_elements_para2=""
    #拼接strict条件参数值
    str_dim_elements_para3=""
    #拼接community条件参数值
    str_dim_elements_para4=""
    #拼接where条件参数值
    str_dim_value=""
    #拼接where查询条件参数语句部分
    str_condition_join_para=""
    #拼接groupby、orderby后面的条件参数
    str_dimlb_join_para=""
    str_dim_elements_lists=[]
    #拼接province查询参数值
    str_xzqh_queryss_parass1=""
    #拼接qx查询参数值
    str_xzqh_queryss_parass2=""
    #拼接strict查询参数值
    str_xzqh_queryss_parass3=""
    #拼接community查询参数值
    str_xzqh_queryss_parass4=""
    #xzqh标识参数
    str_dim_bs_para=""
    for dim in dict_unjoin_data["dim_info"].keys():
        #维表信息
        str_dim_info = "dim_"+str_theme+"_"+dim
        str_dim_id_info = dim+"id"
        #str_select_para = str_select_para+dim+"lb,"
        if dim=="day":
            pass
        else:
            str_left_join_para = str_left_join_para+" left join " +str_dim_info+" on "+str_fact_info+"."+ str_dim_id_info+"="+ str_dim_info+"."+str_dim_id_info
        
        if dim=="xzqh":
            length=len(dict_unjoin_data["dim_info"][dim])
            for j in range(0,length-1):
                str_dim_element_list=dict_unjoin_data["dim_info"][dim][j].split('_')
                print(str_dim_element_list)
                print(len(str_dim_element_list))

                str_xzqh_query_para=str_dim_element_list[-1]

                str_dim_elements_lists.append(str_dim_element_list)


                if len(str_dim_element_list)==1:
                    str_xzqh_queryss_parass1+="'"+str_xzqh_query_para+"'"+","
                elif len(str_dim_element_list)==2:
                    str_xzqh_queryss_parass2+="'"+str_xzqh_query_para+"'"+","
                elif len(str_dim_element_list)==3:
                    str_xzqh_queryss_parass3+="'"+str_xzqh_query_para+"'"+","
                elif len(str_dim_element_list)==4:
                    str_xzqh_queryss_parass4+="'"+str_xzqh_query_para+"'"+","
                else:
                    print("bksx")
                #str_dim_bs_para=dict_unjoin_data["dim_info"][dim][-1]
            if str_xzqh_queryss_parass1:
                temp3=str_xzqh_queryss_parass1.split(",")
                temp3.pop()
                str_dim_element_para1=",".join(temp3)
                str_dim_elements_para1="("+str_dim_element_para1+")"
            if str_xzqh_queryss_parass2:
                temp4=str_xzqh_queryss_parass2.split(",")
                temp4.pop()
                str_dim_element_para2=",".join(temp4)
                str_dim_elements_para2="("+str_dim_element_para2+")"
            if str_xzqh_queryss_parass3:
                temp5=str_xzqh_queryss_parass3.split(",")
                temp5.pop()
                str_dim_element_para3=",".join(temp5)
                str_dim_elements_para3="("+str_dim_element_para3+")"
                print(str_dim_elements_para3)
            if str_xzqh_queryss_parass4:
                temp6=str_xzqh_queryss_parass4.split(",")
                temp6.pop()
                str_dim_element_para4=",".join(temp6)
                str_dim_elements_para4="("+str_dim_element_para4+")"
            else:
                print("没有符合以上条件")
            print("------------------------------------------------------------------------------------------------------")
            #print(str_dim_elements_para)
            print("------------------------------------------------------------------------------------------------------")
            #str_dim_elements_lists.append(str_dim_element_list)
            
            print(str_dim_elements_lists)
            str_dim_bs_para=dict_unjoin_data["dim_info"][dim][0]
            print(str_dim_bs_para)
            print(type(str_dim_bs_para))
            print(dict_unjoin_data["dim_info"][dim][0][2:12])
            print("************************************************************")

            for list_num in range(0,1):
                if dict_unjoin_data["dim_info"][dim][0][2:12]=="0000000000":
                    print("************************************************************")
                    if "provincelb" not in str_condition_join_para:
                        str_condition_join_para=str_condition_join_para+"xzqhbs"+" in "+"("+"'"+str_dim_bs_para+"'"+")"+" and "
                    if "provincelb" not in str_select_para:
                        str_select_para = str_select_para+"provincelb,"
                        str_dimlb_join_para=str_dimlb_join_para+"provincelb,"
                elif dict_unjoin_data["dim_info"][dim][0][6:12]=="000000":
                    if "qxlb" not in str_condition_join_para:
                        str_condition_join_para=str_condition_join_para+"xzqhbs"+" in "+"("+"'"+str_dim_bs_para+"'"+")"+" and "
                    if "qxlb" not in str_select_para:
                        str_select_para = str_select_para+"qxlb,"
                        str_dimlb_join_para=str_dimlb_join_para+"qxlb,"
                elif dict_unjoin_data["dim_info"][dim][0][9:12]=="000":
                    if "strictlb" not in str_condition_join_para:
                        str_condition_join_para=str_condition_join_para+"xzqhbs"+" in "+"("+"'"+str_dim_bs_para+"'"+")"+" and "
                    if "strictlb" not in str_select_para:
                        str_select_para = str_select_para+"strictlb,"
                        str_dimlb_join_para=str_dimlb_join_para+"strictlb,"
                else:
                    if "communitylb" not in str_condition_join_para:
                        str_condition_join_para=str_condition_join_para+"xzqhbs"+" in "+"("+"'"+str_dim_bs_para+"'"+")"+" and "
                    if "communitylb" not in str_select_para:
                        str_select_para = str_select_para+"communitylb,"
                        str_dimlb_join_para=str_dimlb_join_para+"communitylb,"

            print(str_condition_join_para)
            #print(str_xzqh_query_para)
            

        else:
            if dim=="day":
                str_select_para = str_select_para+"day,"
            else:
                str_select_para = str_select_para+dim+"lb,"
            #获取dim字段的key对应的value内容
            for i in dict_unjoin_data["dim_info"][dim]:

                str_dim_value=str_dim_value+"'"+i+"'"+","

            print("str_dim_value="+str_dim_value)
            temp =str_dim_value.split(",")
            #去掉dim字段的key对应的value内容序列最后一个,
            temp.pop()
            str_para =",".join(temp)
            str_paras="("+str_para+")"
            if dim=="day":
                str_condition_join_para=str_condition_join_para+dim+" in "+str_paras +" and "
                str_dimlb_join_para=str_dimlb_join_para+"day,"
            else:
                str_condition_join_para=str_condition_join_para+dim+"lb"+" in "+str_paras +" and "
                str_dimlb_join_para=str_dimlb_join_para+dim+"lb,"
        str_dim_value=""
    temp2=str_dimlb_join_para.split(",")
    temp2.pop()
    str_para2 =",".join(temp2)
    str_left_join_para += " where "+str_condition_join_para+" 1=1 "
    sql = "select "+str_select_para +"count(1) "+"from "+str_fact_info+str_left_join_para+" group by "+str_para2+" order by "+str_para2
    return sql

def doJoinSql7(dict_temp):
    dict_unjoin_data = dict_temp

    str_theme = dict_unjoin_data["theme"]
    #事实表信息
    #str_fact_info = "fact_"+str_theme  
    str_fact_info = "fact_data_"+str_theme
    #拼接“select XXX”部分
    str_select_para = ""
    #拼接“left join”部分
    str_left_join_para = ""
    #截取“xzqh”的参数后的值
    str_dim_element_list=[]
    #拼接“xzqh”的查询参数值
    str_xzqh_query_para=""
    #拼接province条件参数值
    str_dim_elements_para1=""
    #拼接qx条件参数值
    str_dim_elements_para2=""
    #拼接strict条件参数值
    str_dim_elements_para3=""
    #拼接community条件参数值
    str_dim_elements_para4=""
    #拼接where条件参数值
    str_dim_value=""
    #拼接where查询条件参数语句部分
    str_condition_join_para=""
    #拼接groupby、orderby后面的条件参数
    str_dimlb_join_para=""
    str_dim_elements_lists=[]
    #拼接province查询参数值
    str_xzqh_queryss_parass1=""
    #拼接qx查询参数值
    str_xzqh_queryss_parass2=""
    #拼接strict查询参数值
    str_xzqh_queryss_parass3=""
    #拼接community查询参数值
    str_xzqh_queryss_parass4=""
    #xzqh标识参数
    str_dim_bs_para=""
    for dim in dict_unjoin_data["dim_info"].keys():
        #维表信息
        str_dim_info = "dim_"+str_theme+"_"+dim
        str_dim_id_info = dim+"id"
        #str_select_para = str_select_para+dim+"lb,"
        if dim=="day":
            pass
        else:
            str_left_join_para = str_left_join_para+" left join " +str_dim_info+" on "+str_fact_info+"."+ str_dim_id_info+"="+ str_dim_info+"."+str_dim_id_info

        if dim=="xzqh":
            length=len(dict_unjoin_data["dim_info"][dim])
            for j in range(0,length-1):
                str_dim_element_list=dict_unjoin_data["dim_info"][dim][j].split('_')
                print(str_dim_element_list)
                print(len(str_dim_element_list))

                str_xzqh_query_para=str_dim_element_list[-1]

                str_dim_elements_lists.append(str_dim_element_list)


                if len(str_dim_element_list)==1:
                    str_xzqh_queryss_parass1+="'"+str_xzqh_query_para+"'"+","
                elif len(str_dim_element_list)==2:
                    str_xzqh_queryss_parass2+="'"+str_xzqh_query_para+"'"+","
                elif len(str_dim_element_list)==3:
                    str_xzqh_queryss_parass3+="'"+str_xzqh_query_para+"'"+","
                elif len(str_dim_element_list)==4:
                    str_xzqh_queryss_parass4+="'"+str_xzqh_query_para+"'"+","
                else:
                    print("xzqh的参数长度错误")
                #str_dim_bs_para=dict_unjoin_data["dim_info"][dim][-1]
            if str_xzqh_queryss_parass1:
                temp3=str_xzqh_queryss_parass1.split(",")
                temp3.pop()
                str_dim_element_para1=",".join(temp3)
                str_dim_elements_para1="("+str_dim_element_para1+")"
        if str_xzqh_queryss_parass2:
                temp4=str_xzqh_queryss_parass2.split(",")
                temp4.pop()
                str_dim_element_para2=",".join(temp4)
                str_dim_elements_para2="("+str_dim_element_para2+")"
            if str_xzqh_queryss_parass3:
                temp5=str_xzqh_queryss_parass3.split(",")
                temp5.pop()
                str_dim_element_para3=",".join(temp5)
                str_dim_elements_para3="("+str_dim_element_para3+")"
                print(str_dim_elements_para3)
            if str_xzqh_queryss_parass4:
                temp6=str_xzqh_queryss_parass4.split(",")
                temp6.pop()
                str_dim_element_para4=",".join(temp6)
                str_dim_elements_para4="("+str_dim_element_para4+")"
            else:
                print("没有符合条件的参数")

            #print(str_dim_elements_lists)
            for dim_value_num_para in range(0,len(dict_unjoin_data["dim_info"][dim])):
                str_dim_bs_para=str_dim_bs_para+"'"+dict_unjoin_data["dim_info"][dim][dim_value_num_para]+"'"+","
            temp6=str_dim_bs_para.split(",")
            temp6.pop()
            str_dim_bs_para2=",".join(temp6)

            #print(str_dim_bs_para)
            #print(type(str_dim_bs_para))
            #print(dict_unjoin_data["dim_info"][dim][0][2:12])

            for list_num in range(0,len(dict_unjoin_data["dim_info"][dim])):
                #str_dim_bs_para=dict_unjoin_data["dim_info"][dim][list_num]
                if dict_unjoin_data["dim_info"][dim][0][2:12]=="0000000000":
                    if "provincelb" not in str_condition_join_para:
                        str_condition_join_para=str_condition_join_para+"xzqhbs"+" in "+"("+"'"+str_dim_bs_para+"'"+")"+" and "
                    if "provincelb" not in str_select_para:
                        str_select_para = str_select_para+"provincelb,"
                        str_dimlb_join_para=str_dimlb_join_para+"provincelb,"
                elif dict_unjoin_data["dim_info"][dim][0][6:12]=="000000":
                    if "qxlb" not in str_condition_join_para:
                        str_condition_join_para=str_condition_join_para+"xzqhbs"+" in "+"("+"'"+str_dim_bs_para+"'"+")"+" and "
                    if "qxlb" not in str_select_para:
                        str_select_para = str_select_para+"qxlb,"
                        str_dimlb_join_para=str_dimlb_join_para+"qxlb,"
                elif dict_unjoin_data["dim_info"][dim][0][9:12]=="000":
                    if "strictlb" not in str_condition_join_para:
                        str_condition_join_para=str_condition_join_para+"xzqhbs"+" in "+"("+"'"+str_dim_bs_para+"'"+")"+" and "
                    if "strictlb" not in str_select_para:
                        str_select_para = str_select_para+"strictlb,"
                        str_dimlb_join_para=str_dimlb_join_para+"strictlb,"
                else:
                    if "communitylb" not in str_condition_join_para:
                        str_condition_join_para=str_condition_join_para+"xzqhbs"+" in "+"("+str_dim_bs_para2+")"+" and "
                    if "communitylb" not in str_select_para:
                        str_select_para = str_select_para+"communitylb,"
                        str_dimlb_join_para=str_dimlb_join_para+"communitylb,"

            print(str_condition_join_para)
            #print(str_xzqh_query_para)


        else:
            if dim=="day":
                str_select_para = str_select_para+"day,"
            else:
                str_select_para = str_select_para+dim+"lb,"
            #获取dim字段的key对应的value内容
            for i in dict_unjoin_data["dim_info"][dim]:

                str_dim_value=str_dim_value+"'"+i+"'"+","

            print("str_dim_value="+str_dim_value)
            temp =str_dim_value.split(",")
            #去掉dim字段的key对应的value内容序列最后一个,
            temp.pop()
            str_para =",".join(temp)
            str_paras="("+str_para+")"
            if dim=="day":
                str_condition_join_para=str_condition_join_para+dim+" in "+str_paras +" and "
                str_dimlb_join_para=str_dimlb_join_para+"day,"
            else:
                str_condition_join_para=str_condition_join_para+dim+"lb"+" in "+str_paras +" and "
                str_dimlb_join_para=str_dimlb_join_para+dim+"lb,"
        str_dim_value=""
    temp2=str_dimlb_join_para.split(",")
    temp2.pop()
    str_para2 =",".join(temp2)
    str_left_join_para += " where "+str_condition_join_para+" 1=1 "
    sql = "select "+str_select_para +"count(1) "+"from "+str_fact_info+str_left_join_para+" group by "+str_para2+" order by "+str_para2
    return sql
