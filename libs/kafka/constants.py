LOGIN_EVENT = 'login-event'
GENERIC_EVENT = 'generic-event'

EVENT_TOPIC_MAPPING = {
    LOGIN_EVENT: 'user-logins',
    GENERIC_EVENT: 'generic-events'
}

EVENT_CONSUMER_GROUPS = {
    LOGIN_EVENT: 'login-consumers',
    GENERIC_EVENT: 'generic-events'
}
