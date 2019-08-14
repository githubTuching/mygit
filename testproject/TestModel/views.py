
from django.shortcuts import render


from impala.dbapi import connect
from urllib.parse import parse_qs
from django.http import HttpResponse
from django.http.response import JsonResponse
from django.http import QueryDict
import json


def search_info(request):
    if request.method == 'GET':
        return render(request,"TestModel/hello.html",{"hello":"pass"})
    elif request.method =='POST':
        try:
            #接收请求的信息
            info=request.body
            data4info = json.loads(info)
            sql=doJoinSql(data4info)
            conn = connect(host='130.10.7.108', port=21050)
            cursor = conn.cursor()
            cursor.execute('USE rk')
            cursor.execute(sql)
            #查询信息结果
            results = cursor.fetchall()
            print(results)
            ret={
                'msg': testmsg(results),
                'code': testcode(results),
                'data': results2json(data4info,results)
                }
            #return JsonResponse(ret,safe=False)
        except Exception as e:
            print("**************************************")
            print(e)
            print("**************************************")
            print("^上述异常发生了^")
        else:
            print("**************************************")
            print("一 切 OK")
            print("**************************************")
        finally:
            return JsonResponse(ret,safe=False)
    else:
        return "请求方法错误"

def results2json(dict_temp,results):
    #将请求的json格式的信息的"theme"加入字典
    str_theme = dict_temp["theme"]
    #依据返回的数据以组为单位拼接数据
    join_team_data_para=""
    #将拼接成每组数据转成json
    join_team_jsondata_para=""
    #存放join_team_listdata_para的list
    join_teams_listsdata_para=[]
    #用于循环的随机变量
    time=-1
    #添加返回结果的各条信息的总数的key即"count"
    dict_temp["dim_info"]["count"]=""
    for i in range(0,len(results)):
        for j in range(0,len(results[i])):
            for dim in dict_temp["dim_info"].keys():
                time+=1
                if time < j:
                    continue
                #依据返回的数据以组为单位拼接数据                
                join_team_data_para=join_team_data_para+'"'+dim+'"'+":"+'"'+str(results[i][j])+'"'+", "
                time=-1
                break
        #join_team_jsondata_para=join_team_jsondata_para+"{"+join_team_data_para+"}"+", "
        #将拼接的每组数据以逗号(,)进行切割
        temp0=join_team_data_para.split(",")
        #将拼接的每组数据的最后一个逗号(,)去掉
        temp0.pop()
        join_team_data_para=",".join(temp0)
        join_team_data_para='{'+join_team_data_para+'}'
        #将拼接的每组字符串转化成json格式
        join_team_jsondata_para=json.loads(join_team_data_para)
        #将每组json格式的数据以元素的形式加入列表
        join_teams_listsdata_para.append(join_team_jsondata_para)
        join_team_data_para=""
    #join_teams_listsdata_para="["+join_team_jsondata_para+"]"
    return join_teams_listsdata_para
#服务器返回的信息
def testmsg(results1):
    msg=""
    if results1 is not None:
        msg="请求成功"
    else:
        msg="请求失败"
    return msg
#服务器返回的编码
def testcode(results1):
    code=-1
    if results1 is not None:
        code=0
    else:
        code=1
    return code
#将请求的数据进行sql拼接查询
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
        if dim=="day":
            pass
        else:
            str_left_join_para = str_left_join_para+" left join " +str_dim_info+" on "+str_fact_info+"."+ str_dim_id_info+"="+ str_dim_info+"."+str_dim_id_info
        if dim=="xzqh":
            #xzqh的参数个数
            length=len(dict_unjoin_data["dim_info"][dim])
            for j in range(0,length):
                #xzqh的每一个参数按下划线(_)划分,返回一个列表
                str_dim_element_list=dict_unjoin_data["dim_info"][dim][j].split('_')
                print(str_dim_element_list)
                print(len(str_dim_element_list))
                #获取每个参数列表最后一个值
                str_xzqh_query_para=str_dim_element_list[-1]
                #xzqh每个参数形成的列表的集合
                str_dim_elements_lists.append(str_dim_element_list)

                #以下为xzqh每个参数切割之后形成的列表长度的情况判断
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
            #province查询参数
            if str_xzqh_queryss_parass1:
                #使用逗号(,)来切割拼接的province查询参数值
                temp3=str_xzqh_queryss_parass1.split(",")
                #去掉最后一个逗号(,)
                temp3.pop()
                #将切割后的province查询参数值以逗号(,)分割
                str_dim_element_para1=",".join(temp3)
                str_dim_elements_para1="("+str_dim_element_para1+")"
            #qx查询参数
            if str_xzqh_queryss_parass2:
                temp4=str_xzqh_queryss_parass2.split(",")
                temp4.pop()
                str_dim_element_para2=",".join(temp4)
                str_dim_elements_para2="("+str_dim_element_para2+")"
            #strict查询参数
            if str_xzqh_queryss_parass3:
                temp5=str_xzqh_queryss_parass3.split(",")
                temp5.pop()
                str_dim_element_para3=",".join(temp5)
                str_dim_elements_para3="("+str_dim_element_para3+")"
            #community查询参数
            if str_xzqh_queryss_parass4:
                temp6=str_xzqh_queryss_parass4.split(",")
                temp6.pop()
                str_dim_element_para4=",".join(temp6)
                str_dim_elements_para4="("+str_dim_element_para4+")"
            else:
                print("没有符合条件的参数")
            #list_num为xzqh每个参数形成的列表的集合的长度内的步长
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

            #print(str_condition_join_para)


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
            #去掉dim字段的key对应的value内容序列最后一个逗号(,)
            temp.pop()
            str_para =",".join(temp)
            str_paras="("+str_para+")"
            #请求的json格式的数据转化成字典时的key为”day“时的情况
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


