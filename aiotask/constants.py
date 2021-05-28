import enum


class TlvType(enum.IntEnum):
    """ Type of a network TLV """

    ## Enqueue a task on the server
    ENQUEUE = 1

    ## Update the client on the status of a task
    UPDATE = 2

    ## Hand back a task result to the client
    DONE = 3

    ## Explicitly ask the server for a task status
    STATUS = 4


class TaskStatus(enum.IntEnum):
    """ Status of a task """

    ## Task is in the processing queue
    IN_QUEUE = 0

    ## Task is being run
    PROCESSING = 1

    ## Task is processed
    DONE = 2

    ## Task hasn't been sent to server yet
    NOT_SUBMITTED = 254

    ## Task does not exist
    NOT_FOUND = 255
