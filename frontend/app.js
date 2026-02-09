const API_BASE = "/api";

const API_ENDPOINTS = {
  GET_TRAJECTORY: `${API_BASE}/trajectory`,
  ANALYZE: `${API_BASE}/analyze`
};

const ANIMATION_DURATION = 5000;
const INSTRUCTION_DURATION = 3000;
const MARGIN = 20;

let canvas, ctx;
let instructionScreen, usernameScreen;
let trajectoryPoints = [];
let animationStartTime = null;
let animationRequestId = null;
let mediaStream = null;
let mediaRecorder = null;
let recordedChunks = [];
let username = null;

document.addEventListener("DOMContentLoaded", initApp);

function initApp() {
  canvas = document.getElementById("trajectoryCanvas");
  ctx = canvas.getContext("2d");
  instructionScreen = document.getElementById("instructionScreen");
  usernameScreen = document.getElementById("usernameScreen");

  const startBtn = document.getElementById("startBtn");
  const usernameInput = document.getElementById("usernameInput");

  startBtn.onclick = () => {
    const val = usernameInput.value.trim();
    if (!val) {
      alert("Введите ник");
      return;
    }
    username = val;
    usernameScreen.classList.add("hidden");
    startAll();
  };

  resizeCanvas();
  window.addEventListener("resize", resizeCanvas);
}

function resizeCanvas() {
  canvas.width = window.innerWidth - MARGIN * 2;
  canvas.height = window.innerHeight - MARGIN * 2;
}

async function startAll() {
  if (animationRequestId) {
    cancelAnimationFrame(animationRequestId);
    animationRequestId = null;
  }

  ctx.clearRect(0, 0, canvas.width, canvas.height);
  trajectoryPoints = [];
  animationStartTime = null;

  await loadTrajectory();

  instructionScreen.classList.remove("hidden");
  setTimeout(() => {
    instructionScreen.classList.add("hidden");
    startRecordingAndAnimation();
  }, INSTRUCTION_DURATION);
}

async function loadTrajectory() {
  const r = await fetch(API_ENDPOINTS.GET_TRAJECTORY, { method: "POST" });
  const data = await r.json();
  trajectoryPoints = data.trajectory;
}

async function startRecordingAndAnimation() {
  ctx.clearRect(0, 0, canvas.width, canvas.height);

  mediaStream = await navigator.mediaDevices.getUserMedia({ video: true });
  recordedChunks = [];

  mediaRecorder = new MediaRecorder(mediaStream, { mimeType: "video/webm" });
  mediaRecorder.ondataavailable = e => e.data.size && recordedChunks.push(e.data);
  mediaRecorder.onstop = onRecordingStop;

  mediaRecorder.start();
  animationStartTime = null;
  animationRequestId = requestAnimationFrame(animate);
}

function animate(ts) {
  if (!animationStartTime) animationStartTime = ts;
  const t = Math.min((ts - animationStartTime) / ANIMATION_DURATION, 1);
  drawFrame(t);

  if (t < 1) {
    animationRequestId = requestAnimationFrame(animate);
  } else {
    mediaRecorder.stop();
  }
}

async function onRecordingStop() {
  mediaStream.getTracks().forEach(t => t.stop());

  const blob = new Blob(recordedChunks, { type: "video/webm" });
  const formData = new FormData();

  formData.append("video", blob, "record.webm");
  formData.append("trajectory", JSON.stringify(trajectoryPoints));
  formData.append("username", username);

  await fetch(API_ENDPOINTS.ANALYZE, {
    method: "POST",
    body: formData
  });

  console.log("Done");
}

function drawFrame(t) {
  ctx.clearRect(0, 0, canvas.width, canvas.height);

  if (trajectoryPoints.length === 0) return;

  const xs = trajectoryPoints.map(p => p.x);
  const ys = trajectoryPoints.map(p => p.y);
  const minX = Math.min(...xs), maxX = Math.max(...xs);
  const minY = Math.min(...ys), maxY = Math.max(...ys);

  const scaleX = (canvas.width - 2 * MARGIN) / (maxX - minX || 1);
  const scaleY = (canvas.height - 2 * MARGIN) / (maxY - minY || 1);
  const scale = Math.min(scaleX, scaleY);

  const offsetX = canvas.width / 2 - ((minX + maxX) / 2) * scale;
  const offsetY = canvas.height / 2 + ((minY + maxY) / 2) * scale; 

  ctx.beginPath();
  trajectoryPoints.forEach((p, i) => {
    const x = offsetX + p.x * scale;
    const y = offsetY - p.y * scale;
    i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y);
  });
  ctx.strokeStyle = "#0066cc";
  ctx.lineWidth = 2;
  ctx.stroke();

  const p = getPointAt(t);
  if (p) {
    ctx.beginPath();
    const x = offsetX + p.x * scale;
    const y = offsetY - p.y * scale;
    ctx.arc(x, y, 8, 0, Math.PI * 2);
    ctx.fillStyle = "red";
    ctx.fill();
  }
}

function getPointAt(t) {
  const i = Math.floor(t * (trajectoryPoints.length - 1));
  const j = Math.min(i + 1, trajectoryPoints.length - 1);
  const k = t * (trajectoryPoints.length - 1) - i;

  return {
    x: trajectoryPoints[i].x * (1 - k) + trajectoryPoints[j].x * k,
    y: trajectoryPoints[i].y * (1 - k) + trajectoryPoints[j].y * k
  };
}



