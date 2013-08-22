#!/usr/bin/python
import zmq
import json
import time
ZMQ_SERVER = 'tcp://*:1980'

class Handler:

  def __init__(self):
    self.worker_queue = []
    self.task_queue = []
    self.actions_registry = {}

  def host(self):
    print('[Starting 0MQ Host]...')
    context = zmq.Context()
    self.socket = context.socket(zmq.REP)
    self.socket.bind(ZMQ_SERVER)

  def listen(self):
    print('[Listening]...')
    request = json.loads(self.socket.recv())
    if (request['type'] == 'alive'):
      self.handle_alive(request)
    elif (request['type'] == 'request_task'):
      self.handle_request_task(request)
    elif (request['type'] == 'new_task'):
      self.handle_new_task(request)
    elif (request['type'] == 'completed_task'):
      self.handle_completed_task(request)
    elif (request['type'] == 'change_task'):
      self.handle_change_task(request)
    elif (request['type'] == 'executing_task'):
      self.handle_executing_task(request)
    elif (request['type'] == 'delete_task'):
      self.handle_delete_task(request)
    else:
      self.handle_bad_request(request)
    
  ## Handles 'alive'
  def handle_alive(self, request):
    print('--> %s is alive, adding to network' % request['id'])
    self.worker_queue.append(request)
    response = json.dumps({'type':'alive', 'status':'ok'})
    self.socket.send(response)

  ## Handles 'request_task'
  def handle_request_task(self, request):
    print('--> %s needs a task' % request['id'])
    if self.task_queue:
      print('--> Searching for task-worker match')
      for worker in self.worker_queue:
        for task in self.task_queue:
          if (worker['equipment'] == task['requires']):
            print('--> Found task for worker')
            self.actions_registry[request['id']] = task
            self.task_queue.remove(task)
            break
    else:
      print('--> No appropriate tasks')
      task = {'id':0,'description':'Wait'}
      self.actions_registry[request['id']] = task
    print('--> Assigning task: %s' % task['id'])
    print('--> Updating action registery: \n %s' % self.actions_registry) 
    response = json.dumps({'type':'assign', 'task':task})
    self.socket.send(response)

  ## Handles 'new_task'
  def handle_new_task(self, request):
    print('--> %s added a new task: %s' % (request['id'], request['task']['id']))
    self.task_queue.append(request['task'])
    response = json.dumps({'type':'new_task', 'status':'ok'})
    self.socket.send(response)

  ## Handles 'completed_task'
  def handle_completed_task(self, request):
    print('--> %s completed a task: %s' % (request['id'], request['task']['id']))
    self.actions_registry[request['id']] = None
    response = json.dumps({'type':'completed_task', 'status':'ok'})
    self.socket.send(response)

  ## Handles 'change_task', adds a new task and changes the worker to that task
  ## The worker will still complete any non-assigned tasks still in the queue
  def handle_change_task(self, request):
    print('--> %s requested a task change' % request['id'])
    worker = (worker for worker in self.worker_queue if worker['id'] == request['worker']).next()
    if (request['task']['requires'] == worker['equipment']):
      print('---> %s is able to do: %s' % (request['worker'], request['task']['id']))
      response = json.dumps({'type':'change_task', 'status':'ok'})
      self.actions_registry[request['worker']] = request['task']
    else:
      response = json.dumps({'type':'change_task', 'status':'not equipped'})
    self.socket.send(response)

  ## Handles 'executing_task', changes task if necessary
  def handle_executing_task(self, request):
    if (request['task']['id'] == self.actions_registry[request['id']]['id']):
      print('--> %s is executing task: %s' % (request['id'], request['task']['id']))
      response = json.dumps({'type':'executing_task', 'status':'ok'})
    else:
      print('--> %s will change to task: %s' % (request['id'], self.actions_registry[request['id']]['id']))
      response = json.dumps({'type':'change_task', 'status':'ok', 'task':self.actions_registry[request['id']]})
    self.socket.send(response)

  ## Handles 'delete_task
  def handle_delete_task(self, request):
    print('--> %s wants to delete task: %s' % (request['id'], request['task']['id']))
    self.task_queue.remove(request['task'])
    response = json.dumps({'type':'delete_task', 'status':'ok'})
    self.socket.send(response)

  ## Handles bad request
  def handle_bad_request(self, request):
    print('--> %s made a bad request' % request['id'])
    response = json.dumps({'type':'error', 'status':'bad'})
    self.socket.send(response)

if __name__ == '__main__':
  server = Handler()
  server.host()
  while True:
    print('---------------------')
    server.listen()
    time.sleep(1)
