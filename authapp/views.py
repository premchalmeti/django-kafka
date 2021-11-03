import json
from django.shortcuts import render
from django.http import HttpResponse

from libs.kafka.utils import produce
from libs.kafka import constants

# Create your views here.


def login(request):
    if request.method == 'GET':
        return render(request, 'auth/login.html')
    else:
        username = request.POST.get('username')
        pwd = request.POST.get('pwd')

        if username == 'premkumar.chalmeti' and pwd == 'dummypass':
            produce(
                constants.LOGIN_EVENT, 
                key=username, data=json.dumps({'username': username})
            )
            # produce to kafka user has been authenticated
            return render(request, 'auth/login.html', context={'msg': 'Logged in successfully!'})
        else:
            return render(request, 'auth/login.html', context={'msg': 'Invalid credentials'})
