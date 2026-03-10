const audio = document.getElementById("audioPlayer");
const playerWrap = document.querySelector(".player-wrap");

function setPlayerVisible(isVisible) {
  if (!playerWrap) return;
  playerWrap.classList.toggle("active", Boolean(isVisible));
}

async function jumpToTime(url, seconds) {
  const safeUrl = String(url || "");
  const rawSeconds = Number(seconds || 0);
  const safeSeconds = Number.isFinite(rawSeconds)
    ? rawSeconds > 100000
      ? rawSeconds / 1000
      : rawSeconds
    : 0;

  if (!safeUrl) return;

  setPlayerVisible(true);

  if (audio.src !== safeUrl) {
    audio.src = safeUrl;
    await new Promise((resolve) => {
      audio.addEventListener("loadedmetadata", resolve, { once: true });
    });
  }

  audio.currentTime = Math.max(0, safeSeconds);
  await audio.play();
}

document.addEventListener("click", async (event) => {
  const target = event.target;

  if (!(target instanceof HTMLElement)) return;
  if (!target.classList.contains("play-from")) return;

  try {
    await jumpToTime(target.dataset.url, target.dataset.seconds);
  } catch {
    alert("تعذّر تشغيل الصوت الآن. جرّب مرة أخرى.");
  }
});

audio.addEventListener("play", () => setPlayerVisible(true));
audio.addEventListener("loadedmetadata", () => setPlayerVisible(true));

if (audio.src) {
  setPlayerVisible(true);
}

window.jumpToTime = jumpToTime;