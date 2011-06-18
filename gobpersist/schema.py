from __future__ import absolute_import
import .fields

class Workspace(model.Model):
    workspace_id = fields.UUID(primary_key=True)
    name = models.CharField(max_length=255)
    parent = models.ForeignKey('self', related_name="parent_workspace", null=True)
    create_time = models.DateTimeField()
    expiry_time = models.DateTimeField()
    family = models.ForeignKey('self', related_name="family_workspace")
    raw_permalink = models.CharField(max_length=255)
    domain_allow_type = fields.EnumerationField(db_type='domain_allow_enum', choices=['none', 'collaborator', 'viewer'])
    viewer_auth_type = fields.EnumerationField(db_type='viewer_auth_enum', choices=['auth', 'wildcard'])
    viewer_exclude_list = models.TextField(default="")
    status_flags = models.IntegerField(default=0)
    flags = models.IntegerField(default=0)
    delete_time = models.DateTimeField()
    description = models.TextField()
    creator = models.CharField(max_length=255)
