from django.urls import path

from . import views

urlpatterns = [
    path('', views.index, name='index'),
    path('first_login', views.first_login, name='first_login'),
]
