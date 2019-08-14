from django.conf.urls import url
from . import views
urlpatterns = [
    #url(r'^index/$', views2.index),
    #url(r'^search/$', views2.search),
    #url(r'^search2/$', views2.search2),
    #url(r'^search3/$', views2.search3),
    url(r'^search_info/$', views.search_info),
    #url(r'^search_info2/$', views.search_info2),
    url(r'^doJoinSql/$', views.doJoinSql),
    #url(r'^doJoinSql2/$', views2.doJoinSql2)
]

