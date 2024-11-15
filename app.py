import os

from da_vinci_cdk.application import Application
from da_vinci_cdk.stack import Stack

from omnilake.api.stack import OmniLakeAPIStack

base_dir = Stack.absolute_dir(__file__)

deployment_id = os.getenv('OMNILAKE_DEPLOYMENT_ID', 'dev')

omnilake = Application(
    app_entry=base_dir,
    app_name='omnilake',
    create_hosted_zone=False,
    deployment_id=deployment_id,
    disable_docker_image_cache=True,
    include_event_bus=True,
    log_level='DEBUG',
)

omnilake.add_uninitialized_stack(OmniLakeAPIStack)

omnilake.synth()