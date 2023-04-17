from django.http import JsonResponse
 
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated


@api_view(['GET'])
@permission_classes([IsAuthenticated])
def hello(request):
    user = request.user
    print(user)
    response = {'message': 'Hello, World!'}
    return JsonResponse(response)
 