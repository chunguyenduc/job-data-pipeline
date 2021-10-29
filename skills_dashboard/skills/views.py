from skills.models import Skill, Job, SkillModel
from skills.serializers import SkillSerializer, JobSerializer, JobListQuerySerializer
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

    @swagger_auto_schema(
        query_serializer=JobListQuerySerializer,
        responses={200: JobSerializer(many=True)},
    )
    def get(self, request, *args, **kwargs):
        queryset = self.filter_queryset(self.get_queryset())

        serializer = self.get_serializer(queryset, many=True)
        return Response(serializer.data)

    def get_queryset(self):
        """
        Optionally restricts the returned purchases to a given user,
        by filtering against a `username` query parameter in the URL.
        """
        queryset = Job.objects.all()
        city_value = self.request.query_params.get('city')
        title_value = self.request.query_params.get('title')
        skill_value = self.request.query_params.getlist('skills')
        print(city_value, title_value, skill_value)
        if city_value is not None:
            queryset = Job.objects.filter(city__iexact=city_value)
        if title_value is not None:
            queryset = Job.objects.filter(title__icontains=title_value)
        return queryset
