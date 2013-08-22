#!/usr/bin/python
import zmq
import json
import time
ZMQ_SERVER = 'tcp://127.0.0.1:1980'
WORKER_ID = 'worker1'
WORKER_TASK = {}
WORKER_ATTRIBUTES = {
                    'equipment':'cultivator12'
                    }

class Worker:
  
  ## Start
  def __init__(self):
    self.worker_id = WORKER_ID
    self.task = WORKER_TASK
    self.attributes = WORKER_ATTRIBUTES

  ## Connect to server
  def connect(self):
    context = zmq.Context()
    self.socket = context.socket(zmq.REQ)
    self.socket.connect(ZMQ_SERVER)
    print('[Connecting to TaskManager]...')

  ## Request task
  def request_task(self):
    print('[Requesting Task]...')
    message = json.dumps({  
                          'type':'request_task',
                          'worker_id':self.worker_id,
                          'task':self.task,
                          'attributes':self.attributes
                        })
    self.socket.send(message)
    response = json.loads(self.socket.recv())
    self.task = response['task']
    print('--> Received response: %s' % response)

  ## Executing task
  def executing_task(self):
    print('[Executing Task]...')
    print('--> Executing task: %s' % (self.task))
    message = json.dumps({  
                          'type':'executing_task',
                          'worker_id':self.worker_id,
                          'task':self.task,
                          'attributes':self.attributes
                        })
    self.socket.send(message)
    response = json.loads(self.socket.recv())
    print('--> Received response: %s' % response)

  ## Completed task
  def completed_task(self):
    print('[Completed Task]...')
    print('--> Completed task: %s' % (self.task))
    message = json.dumps({  
                          'type':'completed_task',
                          'worker_id':self.worker_id,
                          'task':self.task,
                          'attributes':self.attributes
                        })
    self.socket.send(message)
    response = json.loads(self.socket.recv())
    print('--> Received response: %s' % response)
  
if __name__ == '__main__':
  robot = Worker()
  robot.connect()
  while True:
    print('--------------------')
    robot.request_task()
    robot.executing_task()
    robot.completed_task()
