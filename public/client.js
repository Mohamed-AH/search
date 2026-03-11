const audio = document.getElementById("audioPlayer");
const playerWrap = document.querySelector(".player-wrap");
const feedbackToggle = document.querySelector(".feedback-toggle");
const feedbackPanel = document.querySelector(".feedback-panel");
const feedbackForm = document.querySelector(".feedback-form");
const feedbackStatus = document.querySelector(".feedback-status");
const feedbackInline = document.querySelector(".feedback-inline");
const menuToggle = document.getElementById("menuToggle");
const mobileNav = document.getElementById("mobileNav");
const backdrop = document.getElementById("mobileNavBackdrop");

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

  if (target.classList.contains("play-from")) {
    try {
      await jumpToTime(target.dataset.url, target.dataset.seconds);
    } catch {
      alert("تعذّر تشغيل الصوت الآن. جرّب مرة أخرى.");
    }
    return;
  }
});

audio.addEventListener("play", () => setPlayerVisible(true));
audio.addEventListener("loadedmetadata", () => setPlayerVisible(true));

if (audio.src) {
  setPlayerVisible(true);
}

if (feedbackToggle && feedbackPanel) {
  feedbackToggle.addEventListener("click", () => {
    const isOpen = feedbackPanel.classList.toggle("active");
    feedbackToggle.setAttribute("aria-expanded", String(isOpen));
  });
}

let selectedRelevance = null;

if (feedbackForm) {
  feedbackForm.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;
    if (!target.classList.contains("feedback-btn")) return;

    selectedRelevance = target.dataset.relevant;

    feedbackForm.querySelectorAll(".feedback-btn").forEach((btn) => {
      btn.classList.toggle("active", btn === target);
    });
  });
}

const feedbackSend = document.querySelector(".feedback-send");
if (feedbackSend) {
  feedbackSend.addEventListener("click", async () => {
    if (!feedbackForm) return;
    const logId = feedbackForm.dataset.logid;
    const csrfToken = feedbackForm.dataset.csrf;
    if (!logId || selectedRelevance === null || !csrfToken) return;

    const comment = feedbackForm.querySelector("textarea")?.value || "";

    try {
      await fetch("/search/feedback", {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({ logId, relevant: selectedRelevance, comment, csrfToken }),
      });

      if (feedbackInline) {
        feedbackInline.hidden = true;
      }

      if (feedbackStatus) {
        feedbackStatus.textContent = "";
      }
    } catch {
      // ignore feedback errors
    }
  });
}

if (menuToggle && mobileNav && backdrop) {
  function closeMenu() {
    mobileNav.classList.remove("active");
    backdrop.classList.remove("active");
    document.body.style.overflow = "";
  }

  menuToggle.addEventListener("click", () => {
    const open = mobileNav.classList.toggle("active");
    backdrop.classList.toggle("active", open);
    document.body.style.overflow = open ? "hidden" : "";
  });

  backdrop.addEventListener("click", closeMenu);

  mobileNav.querySelectorAll("a").forEach((link) => {
    link.addEventListener("click", closeMenu);
  });
}

window.jumpToTime = jumpToTime;