from skills.models import Skill, Job, SkillModel
from skills.serializers import SkillSerializer, JobSerializer, JobListQuerySerializer
from rest_framework.pagination import PageNumberPagination
from django.http import Http404
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from drf_yasg.utils import swagger_auto_schema
from rest_framework.decorators import api_view, action
from rest_framework import filters
from rest_framework import generics
from collections import OrderedDict


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


class StandardResultsSetPagination(PageNumberPagination):
    page_size = 10
    page_size_query_param = 'page_size'
    max_page_size = 100

    def get_paginated_response(self, data):
        print('paginator : ', len(self.page))
        return Response(OrderedDict([
            ('count', self.page.paginator.count),
            ('page', self.page.number),
            ('page_size', len(self.page)),
            ('next', self.get_next_link()),
            ('previous', self.get_previous_link()),
            ('results', data)
        ]))

    def get_paginated_response_schema(self, schema):
        return {
            'type': 'object',
            'properties': {
                'count': {
                    'type': 'integer',
                    'example': 123,
                },
                'page_size': {
                    'type': 'integer',
                    'example': 123,
                },

                'next': {
                    'type': 'string',
                    'nullable': True,
                    'format': 'uri',
                    'example': 'http://api.example.org/accounts/?{page_query_param}=4'.format(
                        page_query_param=self.page_query_param)
                },
                'previous': {
                    'type': 'string',
                    'nullable': True,
                    'format': 'uri',
                    'example': 'http://api.example.org/accounts/?{page_query_param}=2'.format(
                        page_query_param=self.page_query_param)
                },
                'results': schema,
            },
        }


class JobList(generics.ListAPIView):
    serializer_class = JobSerializer
    pagination_class = StandardResultsSetPagination

    @swagger_auto_schema(
        query_serializer=JobListQuerySerializer,
        responses={200: JobSerializer(many=True)},
    )
    def get(self, request, *args, **kwargs):
        queryset = self.filter_queryset(
            self.get_queryset().order_by('-created_at'))
        page = self.paginate_queryset(queryset)
        # print('Page: ', page)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            # print(serializer)
            return self.get_paginated_response(serializer.data)
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
        skill_value = self.request.query_params.get('skills')
        company_value = self.request.query_params.get('company')
        site_value = self.request.query_params.get('site')
        from_date = self.request.query_params.get('from_date')
        to_date = self.request.query_params.get('to_date')


        print(city_value, title_value, skill_value, company_value, site_value, from_date, to_date)
        if from_date is not None and to_date is not None:
            queryset = queryset.filter(created_at__range=[from_date, to_date])
        if city_value is not None:
            queryset = queryset.filter(city__iexact=city_value)
        if title_value is not None:
            queryset = queryset.filter(title__icontains=title_value)
        if company_value is not None:
            queryset = queryset.filter(company__icontains=company_value)
        if site_value is not None:
            queryset = queryset.filter(site=site_value)
        # if len(skill_value) > 0:
        # if skill_value is not None:
            # queryset = queryset.filter(skills__contains=[skill_value])
        print(queryset.query)
        return queryset
