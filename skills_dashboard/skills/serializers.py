from rest_framework import serializers
from skills.models import Skill, Job, SkillModel
from bson.objectid import ObjectId
from bson.errors import InvalidId
from django.utils.encoding import smart_text
...
class ObjectIdField(serializers.Field):
    """ Serializer field for Djongo ObjectID fields """
    def to_internal_value(self, data):
        # Serialized value -> Database value
        try:
            return ObjectId(str(data))  # Get the ID, then build an ObjectID instance using it
        except InvalidId:
            raise serializers.ValidationError(
                '{} is not a valid ObjectID'.format(data))

    def to_representation(self, value):
        # Database value -> Serialized value
        if not ObjectId.is_valid(value):  # User submitted ID's might not be properly structured
            raise InvalidId
        return smart_text(value)

class SkillSerializer(serializers.ModelSerializer):
    _id = ObjectIdField(read_only=True)
    class Meta:
        model = SkillModel

        fields = ('_id', 'name', )
        # fields = ('id', 'name')

class JobSerializer(serializers.ModelSerializer):
    _id = ObjectIdField(read_only=True)
    skills = serializers.ListField(child=serializers.CharField(max_length=50), required=True)
    class Meta:
        model = Job
        fields = ('_id', 'title', 'city', 'skills', 'city', 'url')

class JobListQuerySerializer(serializers.Serializer):
    title = serializers.CharField(max_length=150, required=False)
    city = serializers.CharField(max_length=100, required=False)
    skills = serializers.ListField(child=serializers.CharField(max_length=50), required=False)
    company = serializers.CharField(max_length=100, required=False)
