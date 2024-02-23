document.addEventListener("DOMContentLoaded", function () {
  loadTasks();
});

// document.getElementById("addTaskBtn").addEventListener("click", function () {
//   openModal("addTaskModal");
// });

// document.getElementById("createTaskBtn").addEventListener("click", createTask);

// function openModal(modalId) {
//   document.getElementById(modalId).style.display = "block";
// }

// function closeModal(modalId) {
//   document.getElementById(modalId).style.display = "none";
// }

// document
//   .getElementById("closeAddTaskModal")
//   .addEventListener("click", function () {
//     closeModal("addTaskModal");
//   });

let currentModalId = null;

document.getElementById("addTaskBtn").addEventListener("click", function () {
  openModal("addTaskModal");
});

document.getElementById("createTaskBtn").addEventListener("click", createTask);

document
  .getElementById("closeAddTaskModal")
  .addEventListener("click", function () {
    closeModal("addTaskModal");
  });

document
  .getElementById("closeEditTaskModal")
  .addEventListener("click", function () {
    closeModal("editTaskModal");
  });

document.getElementById("updateTaskBtn").addEventListener("click", updateTask);

function openModal(modalId) {
  // Close the currently open modal if there is one
  if (currentModalId) {
    closeModal(currentModalId);
  }

  // Open the new modal
  document.getElementById(modalId).style.display = "block";
  currentModalId = modalId;
}

function closeModal(modalId) {
  document.getElementById(modalId).style.display = "none";
  currentModalId = null;
}

function loadTasks() {
  fetch("https://0eosi0ftj1.execute-api.us-east-1.amazonaws.com/v1/tasks")
    .then((response) => response.json())
    .then((data) => {
      displayTasks(data);
    })
    .catch((error) => console.error("Error fetching tasks:", error));
}

function displayTasks(tasks) {
  const tasksContainer = document.getElementById("tasks");
  tasksContainer.innerHTML = "<h2>All Tasks</h2>";

  if (tasks?.data?.length > 0) {
    const table = document.createElement("table");
    table.classList.add("task-table");

    // Create table header
    const tableHeader = document.createElement("tr");
    tableHeader.innerHTML =
      "<th>Title</th><th>Description</th><th>Actions</th>";
    table.appendChild(tableHeader);

    // Create table rows
    tasks.data.forEach((task) => {
      const tableRow = document.createElement("tr");
      tableRow.innerHTML = `
        <td>${task.title}</td>
        <td>${task.description}</td>
        <td class="action__buttons">
          <button onclick="openEditModal('${task.id}', '${task.title}', '${task.description}')">Edit</button>
          <button onclick="deleteTask('${task.id}')">Delete</button>
        </td>
      `;
      table.appendChild(tableRow);
    });

    tasksContainer.appendChild(table);
  } else {
    tasksContainer.innerHTML += "<p>No tasks available.</p>";
  }
}

function createTask() {
  const taskTitle = document.getElementById("taskTitle").value;
  const taskDescription = document.getElementById("taskDescription").value;

  if (taskTitle && taskDescription) {
    const newTask = {
      title: taskTitle,
      description: taskDescription,
    };

    fetch("https://0eosi0ftj1.execute-api.us-east-1.amazonaws.com/v1/task", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(newTask),
    })
      .then((response) => {
        if (!response.ok) {
          throw new Error("Error creating task");
        }
        return response.json();
      })
      .then(() => {
        closeModal("addTaskModal");
        loadTasks();
        clearForm();
      })
      .catch((error) => console.error("Error creating task:", error));
  }
}

function openEditModal(id, title, description) {
  document.getElementById("editTaskId").value = id;
  document.getElementById("editTaskTitle").value = title;
  document.getElementById("editTaskDescription").value = description;
  openModal("editTaskModal");
}

function updateTask() {
  const taskId = document.getElementById("editTaskId").value;
  const taskTitle = document.getElementById("editTaskTitle").value;
  const taskDescription = document.getElementById("editTaskDescription").value;

  if (taskId && taskTitle && taskDescription) {
    const updatedTask = {
      title: taskTitle,
      description: taskDescription,
    };

    fetch(
      `https://0eosi0ftj1.execute-api.us-east-1.amazonaws.com/v1/task?id=${taskId}`,
      {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(updatedTask),
      }
    )
      .then((response) => {
        if (!response.ok) {
          throw new Error("Error updating task");
        }
        return response.json();
      })
      .then(() => {
        closeModal("editTaskModal");
        loadTasks();
      })
      .catch((error) => console.error("Error updating task:", error));
  }
}

function deleteTask(id) {
  const confirmed = confirm("Are you sure you want to delete this task?");

  if (confirmed) {
    fetch(
      `https://0eosi0ftj1.execute-api.us-east-1.amazonaws.com/v1/task?id=${id}`,
      {
        method: "DELETE",
      }
    )
      .then((response) => {
        if (!response.ok) {
          throw new Error("Error deleting task");
        }
        return response.json();
      })
      .then(() => {
        loadTasks();
      })
      .catch((error) => console.error("Error deleting task:", error));
  }
}

function openModal(modalId) {
  document.getElementById(modalId).style.display = "block";
}

function closeModal(modalId) {
  document.getElementById(modalId).style.display = "none";
}

function clearForm() {
  document.getElementById("taskTitle").value = "";
  document.getElementById("taskDescription").value = "";
}
