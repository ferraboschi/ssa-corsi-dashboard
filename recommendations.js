// recommendations.js
// Pure functions. No I/O, no side effects. All inputs passed in.
// Consumed by server.js to compute per-course recommendations from the
// already-cached Shopify course list.

'use strict';

const MIN_STUDENTS_PRESENZA = 6;
const MIN_STUDENTS_ONLINE = 3;

function isOnlineCourse(course) {
  const h = (course.handle || '').toLowerCase();
  const tags = (course.tags || '').toString().toLowerCase();
  return h.includes('online') || tags.includes('online');
}

function minStudentsFor(course) {
  return isOnlineCourse(course) ? MIN_STUDENTS_ONLINE : MIN_STUDENTS_PRESENZA;
}

// Classify a past course by its final enrolment.
//   happened  -> met or exceeded the minimum
//   cancelled -> did not meet the minimum (so we assume it was cancelled
//                or merged into another date; we don't have a ground-truth
//                field for this, so enrolment is our best proxy)
//   unknown   -> we cannot classify (no enrolment data)
function classifyOutcome(course) {
  const enrolled = (course.students || []).length || course.enrollmentCount || 0;
  if (typeof enrolled !== 'number') return 'unknown';
  const min = minStudentsFor(course);
  return enrolled >= min ? 'happened' : 'cancelled';
}

// ---- Date helpers ----

// Parse Shopify created_at / published_at into a JS Date, tolerating nulls.
function toDate(value) {
  if (!value) return null;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : d;
}

// Day difference (b - a), rounded down; negative if b is before a.
function daysBetween(a, b) {
  if (!a || !b) return null;
  const ms = b.getTime() - a.getTime();
  return Math.floor(ms / (1000 * 60 * 60 * 24));
}

// ---- Per-course metrics ----

// Compute raw metrics for a FUTURE course. Caller must pass `courseDate` (a
// Date for when the course runs) and `now` (reference time, usually Date.now).
// `openingDate` is when enrolment became possible — we use Shopify created_at
// as a proxy.
function courseMetrics(course, courseDate, now) {
  const enrolled = (course.students || []).length || course.enrollmentCount || 0;
  const opening = toDate(course.created_at) || toDate(course.published_at);
  const daysSinceOpening = opening ? Math.max(1, daysBetween(opening, now)) : null;
  const daysUntilCourse = courseDate ? daysBetween(now, courseDate) : null;
  const velocity = daysSinceOpening ? enrolled / daysSinceOpening : null;
  const projection = (velocity != null && daysUntilCourse != null && daysUntilCourse > 0)
    ? Math.floor(enrolled + velocity * daysUntilCourse)
    : enrolled;
  return {
    enrolled,
    min: minStudentsFor(course),
    daysSinceOpening,
    daysUntilCourse,
    velocity,
    projection,
  };
}

// ---- Segmentation ----

// Build a coarse segment key for grouping similar courses. Uses:
//   level   — 'certificato' | 'introduttivo' | 'shochu' | 'masterclass' | 'other'
//   online  — boolean
//   educator — raw educator name string, lowercased, or '' if unknown
// City is intentionally NOT in the key because we have too few courses per city;
// city is used for TIE-BREAK only when ranking similarity later.
function segmentKey(course) {
  const h = (course.handle || '').toLowerCase();
  let level = 'other';
  if (h.includes('certificato')) level = 'certificato';
  else if (h.includes('introduttivo')) level = 'introduttivo';
  else if (h.includes('shochu')) level = 'shochu';
  else if (h.includes('masterclass')) level = 'masterclass';
  const online = isOnlineCourse(course);
  const edu = (course.educatorName || '').toLowerCase().trim();
  return [level, online ? 'online' : 'presenza', edu].join('|');
}

// Given a list of past courses (all with known outcome) and a target
// "daysSinceOpening" value, return:
//   { sampleSize, medianAtPoint, p25AtPoint, p75AtPoint,
//     finalMedian, happenedRate }
//
// "enrolledAtPoint" is approximated for past courses: since we don't have a
// time-series of enrolment, we use the FINAL enrolment scaled by the ratio of
// the target daysSinceOpening to the course's total enrolment window. This is
// a coarse approximation that is OK for our use: a linear growth model.
// Callers must treat the returned median as a rough comparator, not ground truth.
function historicalBaselineAtDays(pastCourses, targetDaysSinceOpening) {
  const enrolledAtPoint = [];
  const enrolledFinal = [];
  let happenedCount = 0;

  for (const pc of pastCourses) {
    const opening = toDate(pc.created_at) || toDate(pc.published_at);
    const courseDate = toDate(pc.eventDate); // if we have it; else fall back
    const totalWindow = (opening && courseDate)
      ? Math.max(1, daysBetween(opening, courseDate))
      : null;
    const finalEnrolled = (pc.students || []).length || pc.enrollmentCount || 0;
    enrolledFinal.push(finalEnrolled);
    if (classifyOutcome(pc) === 'happened') happenedCount++;

    if (totalWindow && targetDaysSinceOpening <= totalWindow) {
      // Linear approximation: at targetDays of totalWindow, enrolment ~ final * target/total
      enrolledAtPoint.push(finalEnrolled * (targetDaysSinceOpening / totalWindow));
    } else {
      enrolledAtPoint.push(finalEnrolled);
    }
  }

  const median = (arr) => {
    if (arr.length === 0) return null;
    const s = arr.slice().sort((a, b) => a - b);
    const mid = Math.floor(s.length / 2);
    return s.length % 2 === 0 ? (s[mid - 1] + s[mid]) / 2 : s[mid];
  };
  const percentile = (arr, p) => {
    if (arr.length === 0) return null;
    const s = arr.slice().sort((a, b) => a - b);
    const idx = Math.min(s.length - 1, Math.floor(p * s.length));
    return s[idx];
  };

  return {
    sampleSize: pastCourses.length,
    medianAtPoint: median(enrolledAtPoint),
    p25AtPoint: percentile(enrolledAtPoint, 0.25),
    p75AtPoint: percentile(enrolledAtPoint, 0.75),
    finalMedian: median(enrolledFinal),
    happenedRate: pastCourses.length ? happenedCount / pastCourses.length : null,
  };
}

// ---- Verdict ----

// Given the raw metrics for a FUTURE course + a baseline computed on the
// matching segment of past courses, emit a verdict string.
//
// Verdicts:
//   in-traiettoria      — projection >= min * 1.2  (comfortably above minimum)
//   monitor             — projection >= min        (above minimum but thin)
//   rischio             — projection <  min        (will not make it at current pace)
//   critico             — projection <  min * 0.5  AND daysUntilCourse < 14
//   insufficient-data   — baseline sampleSize < 3 (we do not issue strong verdicts)
//
// Confidence is derived from baseline sample size and how far the data-window
// ratio is from 1:
//   high    — sampleSize >= 7 and daysSinceOpening > 0
//   medium  — sampleSize >= 4
//   low     — everything else
function computeVerdict(metrics, baseline) {
  const { min, projection, daysUntilCourse } = metrics;
  const sampleSize = baseline ? baseline.sampleSize : 0;

  let confidence = 'low';
  if (sampleSize >= 7) confidence = 'high';
  else if (sampleSize >= 4) confidence = 'medium';

  // Without enough history we still say something useful based on projection only,
  // but we flag the lack of context.
  if (sampleSize < 3) {
    if (projection >= min * 1.2) return { verdict: 'in-traiettoria', confidence: 'low' };
    if (projection >= min) return { verdict: 'monitor', confidence: 'low' };
    if (projection < min * 0.5 && daysUntilCourse != null && daysUntilCourse < 14) {
      return { verdict: 'critico', confidence: 'low' };
    }
    return { verdict: 'rischio', confidence: 'low' };
  }

  if (projection < min * 0.5 && daysUntilCourse != null && daysUntilCourse < 14) {
    return { verdict: 'critico', confidence };
  }
  if (projection < min) return { verdict: 'rischio', confidence };
  if (projection >= min * 1.2) return { verdict: 'in-traiettoria', confidence };
  return { verdict: 'monitor', confidence };
}

function buildReasoning(metrics, baseline, course) {
  const parts = [];
  const {
    enrolled, min, velocity, projection, daysSinceOpening, daysUntilCourse,
  } = metrics;
  if (velocity != null) {
    parts.push(
      `Al ritmo di ${velocity.toFixed(2)} iscritti/giorno` +
      (daysUntilCourse != null ? `, tra ${daysUntilCourse} giorni arriveresti a circa ${projection} iscritti.` : '.')
    );
  } else {
    parts.push(`Attualmente ${enrolled} iscritti.`);
  }
  if (baseline && baseline.sampleSize >= 3 && baseline.medianAtPoint != null) {
    parts.push(
      `Corsi simili (${baseline.sampleSize} osservazioni) a ${daysSinceOpening} giorni dall'apertura ` +
      `avevano una mediana di ${baseline.medianAtPoint.toFixed(1)} iscritti.`
    );
    const delta = enrolled - baseline.medianAtPoint;
    if (Math.abs(delta) >= 1) {
      parts.push(
        delta > 0
          ? `Sei ${delta.toFixed(0)} iscritti sopra la mediana storica.`
          : `Sei ${Math.abs(delta).toFixed(0)} iscritti sotto la mediana storica.`
      );
    }
  } else if (baseline && baseline.sampleSize > 0) {
    parts.push(`Solo ${baseline.sampleSize} corsi storici simili: campione troppo piccolo per un confronto solido.`);
  } else {
    parts.push(`Nessun corso storico simile: il sistema non ha ancora basi di confronto per questo tipo.`);
  }
  parts.push(`Minimo per farsi: ${min}.`);
  return parts.join(' ');
}

module.exports = {
  MIN_STUDENTS_PRESENZA,
  MIN_STUDENTS_ONLINE,
  isOnlineCourse,
  minStudentsFor,
  classifyOutcome,
  toDate,
  daysBetween,
  courseMetrics,
  segmentKey,
  historicalBaselineAtDays,
  computeVerdict,
  buildReasoning,
};
