package io.zeebe.aeron.service;

public enum MessageIdentifier {
    JOB_CREATE,
    JOB_CREATED,

    JOB_COMPLETE,

    JOB_EXPIRE,
    JOB_EXPIRED,

//    JOB_ASSIGN,
    JOB_ASSIGNED,

    SUBSCRIBE,
    SUBSCRIBED

}
