{% macro spLogJobStart(ParentJobId,ProcedureName,TopicKey,SubjectKey,PhaseKey=null
    ,DatasetKey=null,SystemKey=null,EntityKey=null) %}
    insert into "UDP_TRANSFORM_DEV"."LOG"."Job"
            (
                 ParentJobId,
                 ProcedureName,
                 TopicKey,
                 SubjectKey,
                 PhaseKey,
                 DatasetKey,
                 SystemKey,
                 EntityKey,
                 JobStatus,
                 StartDateTime
            )
            values(
                 {{ParentJobId}},
                 {{ProcedureName}},
                 {{TopicKey}},
                 {{SubjectKey}},
                 {{PhaseKey}},
                 {{DatasetKey}},
                 {{SystemKey}},
                 {{EntityKey}},
                  'running',
                  getdate()
            )
{% endmacro %}