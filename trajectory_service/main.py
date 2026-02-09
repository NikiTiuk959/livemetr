from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import List, Optional
import trajectory_generator as tg

app = FastAPI(
    title="Trajectory Generator API",
    description="""
    API для генерации траекторий движения стимула на основе ряда Фурье.
    
    ## Возможности
    * Автоматическая генерация случайных траекторий
    * Координаты с началом отсчета в левом верхнем углу экрана
    * Нормированные координаты от 0 до 1
    * Уникальный ID для каждой траектории
    """,
    version="2.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class TrajectoryResponse(BaseModel):
    trajectory_id: str
    points: List[dict]
    parameters: dict

class NormalizedTrajectoryResponse(BaseModel):
    trajectory_id: str
    normalized_points: List[dict]  # Точки от 0 до 1
    parameters: dict

class CombinedTrajectoryResponse(BaseModel):
    trajectory_id: str
    points: List[dict]  # Экранные координаты
    normalized_points: List[dict]  # Нормированные координаты 0-1
    parameters: dict

class FourierCoefficientRequest(BaseModel):
    amplitude: float = Field(..., description="Амплитуда гармоники", example=1.0, ge=0.0)
    frequency: float = Field(..., description="Частота гармоники", example=1.0, ge=0.0)
    phase: float = Field(..., description="Фаза гармоники в радианах", example=0.0)

class CustomTrajectoryRequest(BaseModel):
    screen_width: Optional[float] = Field(default=1920, description="Ширина экрана", ge=100)
    screen_height: Optional[float] = Field(default=1080, description="Высота экрана", ge=100)
    coefficients: Optional[List[FourierCoefficientRequest]] = Field(
        default=None, description="Пользовательские коэффициенты Фурье"
    )

@app.get("/")
async def root():
    return {
        "message": "Trajectory Generator API", 
        "version": "2.1.0",
        "docs": "/docs",
        "redoc": "/redoc"
    }

@app.get("/get_trajectory", response_model=TrajectoryResponse, tags=["Траектории"])
async def get_trajectory():
    """
    Генерация случайной траектории в экранных координатах
    
    Автоматически генерирует траекторию со случайными параметрами:
    - Случайное количество точек (50-200)
    - Случайные коэффициенты Фурье (3-8 гармоник)
    - Координаты с началом отсчета в левом верхнем углу экрана (1920x1080)
    - Уникальный ID траектории
    
    Возвращает массив точек {x, y} и параметры генерации.
    """
    try:
        trajectory = tg.generate_trajectory()
        
        return TrajectoryResponse(
            trajectory_id=trajectory.id,
            points=trajectory.points,
            parameters=trajectory.parameters
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка генерации траектории: {str(e)}")

@app.get("/get_normalized_trajectory", response_model=NormalizedTrajectoryResponse, tags=["Траектории"])
async def get_normalized_trajectory():
    """
    Генерация траектории с нормированными координатами от 0 до 1
    
    Автоматически генерирует траекторию со случайными параметрами:
    - Случайное количество точек (50-200)
    - Случайные коэффициенты Фурье (3-8 гармоник)
    - Координаты нормированы к диапазону [0, 1]
    - Уникальный ID траектории
    
    Возвращает массив точек {x, y} где x,y ∈ [0, 1] и параметры генерации.
    """
    try:
        trajectory = tg.generate_unit_trajectory()
        
        return NormalizedTrajectoryResponse(
            trajectory_id=trajectory.id,
            normalized_points=trajectory.normalized_points,
            parameters=trajectory.parameters
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка генерации траектории: {str(e)}")

@app.get("/get_combined_trajectory", response_model=CombinedTrajectoryResponse, tags=["Траектории"])
async def get_combined_trajectory():
    """
    Генерация траектории с обоими типами координат
    
    Возвращает траекторию одновременно в:
    - Экранных координатах (1920x1080)
    - Нормированных координатах [0, 1]
    
    Удобно для одновременного использования в разных системах.
    """
    try:
        trajectory = tg.generate_trajectory()
        
        return CombinedTrajectoryResponse(
            trajectory_id=trajectory.id,
            points=trajectory.points,
            normalized_points=trajectory.normalized_points,
            parameters=trajectory.parameters
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка генерации траектории: {str(e)}")

@app.get("/get_trajectory_custom_screen", response_model=TrajectoryResponse, tags=["Траектории"])
async def get_trajectory_custom_screen(
    screen_width: float = Query(1920, description="Ширина экрана", ge=100),
    screen_height: float = Query(1080, description="Высота экрана", ge=100)
):
    """
    Генерация траектории с пользовательскими размерами экрана
    
    - **screen_width**: Ширина области отображения
    - **screen_height**: Высота области отображения
    
    Все параметры генерации выбираются случайно.
    Координаты начинаются с (0,0) в левом верхнем углу.
    """
    try:
        trajectory = tg.generate_trajectory_with_custom_screen(
            screen_width=screen_width,
            screen_height=screen_height
        )
        
        return TrajectoryResponse(
            trajectory_id=trajectory.id,
            points=trajectory.points,
            parameters=trajectory.parameters
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка генерации траектории: {str(e)}")

@app.post("/get_trajectory_custom", response_model=TrajectoryResponse, tags=["Траектории"])
async def get_trajectory_custom(request: CustomTrajectoryRequest):
    """
    Генерация траектории с пользовательскими параметрами
    
    Позволяет задать:
    - Размеры экрана
    - Коэффициенты Фурье (опционально)
    
    Если коэффициенты не указаны, генерируются случайные.
    """
    try:
        if request.coefficients:
            # Здесь можно добавить логику для кастомных коэффициентов
            # Пока используем стандартную генерацию
            trajectory = tg.generate_trajectory_with_custom_screen(
                screen_width=request.screen_width,
                screen_height=request.screen_height
            )
        else:
            trajectory = tg.generate_trajectory_with_custom_screen(
                screen_width=request.screen_width,
                screen_height=request.screen_height
            )
        
        return TrajectoryResponse(
            trajectory_id=trajectory.id,
            points=trajectory.points,
            parameters=trajectory.parameters
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка генерации траектории: {str(e)}")

@app.get("/health", tags=["Система"])
async def health_check():
    """Проверка работоспособности API"""
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)