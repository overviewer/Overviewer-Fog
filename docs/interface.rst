Fog Interface
=============


Submitting a render job
-----------------------

def submit_render(world_url:str, render_options:dict, success_callback:str=None, error_callback:str=None) -> str

world_url is a string identifying a zip or tarball of the world to render.  
It should contain only contain 1 world.  

render_options is a dictionary describing the parameters of the render.
The following key/values have meaning:

    * TODO: fill out this section

success_callback is an optional string, specifying a URL to POST to if the render is sucessful.  The following
parameters are included:
    uuid = uuid of the job that just finished successfully

error_callback is an optional string, speciying a URL to POST to if the render has failed with an error.
The following parameters are included:
    uuid = uuid of the job that has failed to render

submit_render() returns a UUID, which can be used to get information about the job.  


Getting information about a job
-------------------------------

def get_job_status(uuid:str) -> dict

uuid is a UUID from submit_render, identifying a job

If there is no such job, returns None (or maybe an empty dictionary)

If a the job exists, a dictionary with the following keys is return:

    * status -- a string (enum?), one of the following:  SUBMITTED, INPROGRESS, COMPLETE, ERROR
    * errmsg -- a string describing the error message, if relevant
    * result -- a string identifying where the rendered map can be found (TODO clarify)



Fog Implementation
==================

TODO describe how we might implement these interfaces

