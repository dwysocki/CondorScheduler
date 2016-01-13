__all__ = [
    "job_template",
    "vars_template",
    "parent_template"
]

job_template = 'JOB {job_id} {condor}\n'.format

vars_template = 'VARS {job_id} input="{input}" arguments="{arguments}"\n'.format

_parent_template = 'PARENT {parents} CHILD {children}\n'.format
def parent_template(*, parents, children):
    return _parent_template(parents=" ".join(parents),
                            children=" ".join(children))

def make_job(job_id, condor, input, arguments):
    return (
        job_template(job_id=job_id, condor=condor) +
        vars_template(job_id=job_id, input=input, arguments=arguments)
    )

dot_template = 'DOT {dot}'.format
