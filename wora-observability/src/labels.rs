use wora::{HealthState, Leadership, ReadinessState};

pub(crate) fn health_value(value: &HealthState) -> i64 {
    match value {
        HealthState::Unknown => 0,
        HealthState::Ok => 1,
        HealthState::Suspended => 2,
        HealthState::TryAgain => 3,
        HealthState::Failed => 4,
    }
}

pub(crate) fn readiness_value(value: &ReadinessState) -> i64 {
    match value {
        ReadinessState::Unknown => 0,
        ReadinessState::NotReady => 1,
        ReadinessState::Ready => 2,
        ReadinessState::Stopping => 3,
        ReadinessState::Draining => 4,
    }
}

pub(crate) fn leadership_value(value: &Leadership) -> i64 {
    match value {
        Leadership::Unknown => 0,
        Leadership::Follower => 1,
        Leadership::Leader => 2,
    }
}
