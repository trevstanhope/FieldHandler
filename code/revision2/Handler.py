#!/usr/bin/python
import zmq
import json
import time
ZMQ_SERVER = 'tcp://*:1980'

class Handler:

  def __init__(self):
    self.task_queue = []
    self.assignments = {}

  def host(self):
    print('[Starting 0MQ Host]...')
    context = zmq.Context()
    self.socket = context.socket(zmq.REP)
    self.socket.bind(ZMQ_SERVER)

  def listen(self):
    print('[Listening]...')
    request = json.loads(self.socket.recv())
    if (request['type'] == 'request_task'):
      self.handle_request_task(request)
    elif (request['type'] == 'executing_task'):
      self.handle_executing_task(request)
    elif (request['type'] == 'completed_task'):
      self.handle_completed_task(request)
    elif (request['type'] == 'new_task'):
      self.handle_new_task(request)
    elif (request['type'] == 'change_task'):
      self.handle_change_task(request)
    elif (request['type'] == 'delete_task'):
      self.handle_delete_task(request)

  ## Handles a task request
  def handle_request_task(self, request):
    print('--> %s is requesting a task.' % request['worker_id'])
    print('--> %s has these attributes: %s' % (request['worker_id'], request['attributes']))
    try:
      task = self.assignments[request['worker_id']]
      print('--> %s was directly assigned a task: %s' % (request['worker_id'], task))
    except Exception:
      for task in self.task_queue:
        print('--> checking task_queue for matches.')
        if (task['requires'] == request['attributes']['equipment']):
          print('--> found match in task_queue.')
          self.task_queue.remove(task) # remove task from queue
          break
      else:
        print('--> no tasks in task_queue.')
        task = {'task_id':0}
    print('--> %s was given this task: %s' % (request['worker_id'], task))
    self.assignments[request['worker_id']] = task
    print('--> updating assignments: %s' % self.assignments)
    response = json.dumps({
                           'type':'request_task',
                           'task':task,
                           'status':'ok'
                          })
    self.socket.send(response)

 ## Handles an execution, changes task if necessary
  def handle_executing_task(self, request):
    print('--> %s is executing a task: %s' % (request['worker_id'], request['task']))
    if (self.assignments[request['worker_id']] == request['task']):
      print('--> %s can keep the same task: %s' % (request['worker_id'], request['task']))
      response = json.dumps({'type':'executing_task', 'status':'ok'})
    else:
      print('--> %s was given a different task: %s' % (request['worker_id'], self.assignments[request['worker_id']]))
      response = json.dumps({'type':'executing_task', 'status':'change'})
    self.socket.send(response)

  ## Handles a completed task
  def handle_completed_task(self, request):
    print('--> %s completed a task: %s' % (request['worker_id'], request['task']))
    del self.assignments[request['worker_id']]
    print('--> updating assignments: %s' % self.assignments)
    response = json.dumps({'type':'completed_task', 'status':'ok'})
    self.socket.send(response)

  ## Handles a new task
  def handle_new_task(self, request):
    print('--> %s added a new task: %s' % (request['user_id'], request['task']))
    self.task_queue.append(request['task'])
    response = json.dumps({'type':'new_task', 'status':'ok'})
    self.socket.send(response)

  ## Handles a task change, adds a new task and changes the worker to that task
  def handle_change_task(self, request):
    print('--> %s requested a task change for %s.' % (request['user_id'], request['worker_id']))
    self.assignments[request['worker_id']] = request['task']
    print('--> updating assignments: %s' % self.assignments)
    response = json.dumps({'type':'change_task', 'status':'ok'})
    self.socket.send(response)

  ## Handles a delete
  def handle_delete_task(self, request):
    print('--> %s wants to delete task.' % request['user_id'])
    response = json.dumps({'type':'delete_task', 'status':'ok'})
    self.socket.send(response)

if __name__ == '__main__':
  server = Handler()
  server.host()
  while True:
    print('---------------------')
    server.listen()
    time.sleep(1)
