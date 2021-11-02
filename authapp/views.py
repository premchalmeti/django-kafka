from django.shortcuts import render
from django.http import HttpResponse

from libs.kafka.producer import produce
from libs.kafka import constants

# Create your views here.


def login(request):
    if request.method == 'GET':
        return render(request, 'auth/login.html')
    else:
        username = request.POST.get('user')
        pwd = request.POST.get('pwd')

        if username == 'premkumar.chalmeti' and pwd == 'dummypass':
            produce(username, {'username': username, 'pwd': pwd})
            return HttpResponse('ok')
            # produce to kafka user has been authenticated
