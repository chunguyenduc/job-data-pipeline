from skills.models import Skill, Job, SkillModel
from skills.serializers import SkillSerializer, JobSerializer
from django.http import Http404
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from drf_yasg.utils import swagger_auto_schema
from rest_framework.decorators import api_view, action
from rest_framework import filters
from rest_framework import generics


class SkillList(APIView):

    def get(self, request):
        skills = SkillModel.objects.all()
        serializer = SkillSerializer(skills, many=True)
        return Response(serializer.data)

    @swagger_auto_schema(method='post', request_body=SkillSerializer)
    @action(detail=False, methods=['post'])
    def post(self, request):
        serializer = SkillSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data, status=status.HTTP_201_CREATED)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

# Create your views here.


class SkillDetail(APIView):
    def get_object(self, pk):
        try:
            return SkillModel.objects.get(pk=pk)
        except SkillModel.DoesNotExist:
            raise Http404

    def get(self, request, pk):
        skill = self.get_object(pk)
        serializer = SkillSerializer(skill)
        return Response(serializer.data)

    @swagger_auto_schema(method='put', request_body=SkillSerializer)
    @action(detail=False, methods=['put'])
    def put(self, request, pk):
        skill = self.get_object(pk)
        serializer = SkillSerializer(skill, data=request.data)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    def delete(self, request, pk):
        skill = self.get_object(pk)
        skill.delete()
        return Response(status=status.HTTP_204_NO_CONTENT)


class JobList(generics.ListAPIView):
    lookup_field = ['city']
    serializer_class = JobSerializer

    def get_queryset(self):
        """
        Optionally restricts the returned purchases to a given user,
        by filtering against a `username` query parameter in the URL.
        """
        city = self.request.query_params.get('city')
        print(city)
        if city is not None:
            queryset = Job.objects.filter(city=city)
            return queryset
        return Job.objects.all()
