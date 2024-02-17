document.getElementById('showInsertForm').addEventListener('click', function() {
    document.getElementById('taskFormContainer').style.display = 'block';
    document.getElementById('deleteTaskForm').style.display = 'none';
    document.getElementById('updateTaskForm').style.display = 'none';
    document.getElementById('tasksDisplay').innerHTML = ''; 
});
document.getElementById('showDeleteUI').addEventListener('click', function() {
    document.getElementById('deleteTaskForm').style.display = 'block';
    document.getElementById('taskFormContainer').style.display = 'none';
    document.getElementById('updateTaskForm').style.display = 'none';
    document.getElementById('tasksDisplay').innerHTML = '';
});
document.getElementById('showUpdateUI').addEventListener('click', function() {
    document.getElementById('updateTaskForm').style.display = 'block';
    document.getElementById('taskFormContainer').style.display = 'none';
    document.getElementById('deleteTaskForm').style.display = 'none';
    document.getElementById('tasksDisplay').innerHTML = '';
});
document.getElementById('updateTask').addEventListener('click', function() {
    const idToUpdate = document.getElementById('updateId').value;
    const newStatus = document.getElementById('updateStatus').value;
    if (!idToUpdate) {
        alert('Please enter a task ID to update.');
        return;
    }

    const requestBody = {
        httpMethod: "put",
        id: idToUpdate,
        update_key: "status",
        update_value: newStatus
    };

    fetch('https://x2dnb3xqk5.execute-api.us-east-1.amazonaws.com/alpha/All_end_points', {
        method: "post",
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(requestBody),
    })
    .then(response => {
        if (!response.ok) {
            throw new Error('Network response was not ok');
        }
        return response.json();
    })
    .then(data => {
        console.log('Update Success:', data);
        alert('Task updated successfully!');
        document.getElementById('updateId').value = '';
        document.getElementById('updateStatus').value = 'Pending';
    })
    .catch((error) => {
        console.error('Error:', error);
        alert('An error occurred while updating the task.');
    });
});

document.getElementById('deleteTask').addEventListener('click', function() {
    const idToDelete = document.getElementById('deleteId').value;
    if (!idToDelete) {
        alert('Please enter a task ID to delete.');
        return;
    }

    const requestBody = {
        httpMethod: "delete",
        id: idToDelete
    };

    fetch('https://x2dnb3xqk5.execute-api.us-east-1.amazonaws.com/alpha/All_end_points', {
        method: "post", 
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(requestBody),
    })
    .then(response => {
        if (!response.ok) {
            throw new Error('Network response was not ok');
        }
        return response.json();
    })
    .then(data => {
        console.log('Delete Success:', data);
        alert('Task deleted successfully!');
        document.getElementById('deleteId').value = '';
    })
    .catch((error) => {
        console.error('Error:', error);
        alert('An error occurred while deleting the task.');
    });
});


document.getElementById('taskForm').addEventListener('submit', function(e) {
    e.preventDefault(); 
    const id = document.getElementById('id').value;
    const todo = document.getElementById('todo').value;
    const status = document.getElementById('status').value;

    const requestBody = {
        httpMethod: "post",
        id: id,
        todo: todo,
        status: status
    };
    fetch('https://x2dnb3xqk5.execute-api.us-east-1.amazonaws.com/alpha/All_end_points', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(requestBody),
    })
    .then(response => response.json())
    .then(data => {
        console.log('Success:', data);
        alert('Task submitted successfully!');
    })
    .catch((error) => {
        console.error('Error:', error);
        alert('An error occurred while submitting the task.');
    });
});

document.getElementById('getAllTasks').addEventListener('click', function() {
    document.getElementById('taskFormContainer').style.display = 'none';
    document.getElementById('deleteTaskForm').style.display = 'none';
    document.getElementById('updateTaskForm').style.display = 'none';
    const requestBodyg = {
        httpMethod: "get"
    };
    // Send a GET request
    fetch('https://x2dnb3xqk5.execute-api.us-east-1.amazonaws.com/alpha/All_end_points', {
        method: "post",
        body: JSON.stringify(requestBodyg),
    })
    .then(responseData => {
        if (!responseData.ok) {
            throw new Error('Network response was not ok');
        }
        return responseData.json();
    })
    .then(data => {
        console.log('Success:', data);
        // Parse the body string to an object
        const parsedData = JSON.parse(data.body);
        const tasks = parsedData.Data; 
    
        const tasksDisplay = document.getElementById('tasksDisplay');
        tasksDisplay.innerHTML = '';
        if (tasks && tasks.length > 0) {
            const list = document.createElement('ul');
            tasks.forEach(task => {
                const item = document.createElement('li');
                item.textContent = `ID: ${task.id}, Todo: ${task.todo}, Status: ${task.status}`;
                list.appendChild(item);
            });
            tasksDisplay.appendChild(list);
        } else {
            tasksDisplay.textContent = 'No tasks found.';
        }
    })
    .catch((error) => {
        console.error('Error:', error);
        alert('An error occurred while fetching the tasks.');
    });
    
});

