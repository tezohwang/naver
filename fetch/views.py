from django.shortcuts import render

# Route Views
def index(request):
    context = {}
    return render(request, 'report/index.html', context)

# Controll Views
